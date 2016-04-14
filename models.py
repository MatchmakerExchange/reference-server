"""
The models module:

Defines conceptual API objects and corresponding methods for parsing/serializing to the API and loading/saving to the index.
"""
from __future__ import with_statement, division, unicode_literals

from copy import deepcopy

import flask
from datastore import DatastoreConnection


class Feature:
    def __init__(self, data):
        self.data = deepcopy(data)
        backend = get_backend()

        # Normalize phenotype term
        term = backend.vocabularies.get_term(id=self.data['id'])
        self.data['id'] = term['id']
        self.data['label'] = term['name']
        self.phenotypes = term['term_category']

        # Normalize age of onset
        term_id = self.data.get('ageOfOnset')
        if term_id:
            term = backend.vocabularies.get_term(id=term_id)
            self.data['ageOfOnset'] = term['id']

        # Normalize observed
        observed = self.data.get('observed', 'yes') == 'yes'
        self.data['observed'] = 'yes' if observed else 'no'

    def get_implied_terms(self):
        return self.phenotypes

    def is_present(self):
        return self.data['observed'] == 'yes'

    def to_json(self):
        return self.data


class Gene:
    def __init__(self, data):
        self.data = deepcopy(data)
        self.term = None
        gene_id = data.get('id')
        if gene_id:
            # Normalize gene id
            backend = get_backend()
            self.term = backend.vocabularies.get_term(id=gene_id)
            self.data['id'] = self.term['id']
            self.data['label'] = self.term['name']

    def get_id(self):
        return self.data.get('id')

    def to_json(self):
        return self.data


class GenomicFeature:
    def __init__(self, data):
        self.data = deepcopy(data)

        # Normalize gene
        gene_json = data.get('gene')
        if gene_json:
            self.gene = Gene(gene_json)
            self.data['gene'] = self.gene.to_json()

        # TODO: Normalize mutation type with SO

    def get_gene_id(self):
        if self.gene:
            return self.gene.get_id()

    def to_json(self):
        return self.data


class Patient:
    def __init__(self, data=None, phenotypes=None, genes=None):
        if data is None:
            data = {}

        if phenotypes is None:
            phenotypes = set()

        if genes is None:
            genes = set()

        # Set of all present and implied features
        self.phenotypes = phenotypes
        # Set of all candidate gene ids
        self.genes = genes
        # API representation of the patient
        self.data = data

    @classmethod
    def from_api(cls, data):
        data = deepcopy(data)
        phenotypes = set()
        genes = set()

        # Normalize phenotype terms
        features = []
        for feature_json in data.get('features', []):
            feature = Feature(feature_json)
            if feature.is_present():
                phenotypes.update(feature.get_implied_terms())

            features.append(feature)

        data['features'] = [feature.to_json() for feature in features]

        # Normalize genomic features
        genomic_features = []
        for gf_json in data.get('genomicFeatures', []):
            gf = GenomicFeature(gf_json)
            gene = gf.get_gene_id()
            if gene:
                genes.add(gene)

            genomic_features.append(gf)

        data['genomicFeatures'] = [gf.to_json() for gf in genomic_features]

        # Normalize test status
        data['test'] = bool(data.get('test', False))

        return cls(data, phenotypes, genes)

    @classmethod
    def from_index(cls, doc):
        obj = cls()
        obj.phenotypes = set(doc['phenotype'])
        obj.genes = set(doc['gene'])
        obj.data = doc['doc']
        return obj

    def get_id(self):
        return self.data['id']

    def to_api(self):
        return self.data

    def to_index(self):
        doc = {
            'phenotype': sorted(self.phenotypes),
            'gene': sorted(self.genes),
            'doc': self.data,
        }
        return doc


class MatchRequest:
    def __init__(self, patient):
        self.patient = patient

    @classmethod
    def from_api(cls, request):
        patient = Patient.from_api(request['patient'])
        return cls(patient)

    def match(self, n=5):
        backend = get_backend()

        phenotypes = self.patient.phenotypes
        genes = self.patient.genes
        hits = backend.patients.match(phenotypes, genes, n=n)

        matches = []
        for hit in hits:
            match = MatchResult(self.patient, hit)
            matches.append(match)

        matches.sort(reverse=True)
        return MatchResponse(matches)


class MatchResult:
    """A simple match view that uses the ElasticSearch score directly"""
    def __init__(self, query_patient, hit):
        self.query_patient = query_patient

        # Parse the patient from the matched doc
        doc = hit['_source']
        self.match_patient = Patient.from_index(doc)

        # Score the match
        self.hit = hit
        self.score = self.get_score()

        # Serialize for API
        self.data = {
            'score': {'patient': self.score},
            'patient': self.match_patient.to_api(),
        }

    def get_score(self):
        # Use the ElasticSearch TF/IDF score, normalized to [0, 1]
        return 1 - 1 / (1 + self.hit['_score'])

    def to_api(self):
        return self.data

    def __lt__(self, other):
        return self.score < other.score


class MatchResponse:
    def __init__(self, matches):
        self.matches = matches
        self.data = {
            'results': [match.to_api() for match in matches],
        }

    def to_api(self):
        return self.data


def get_backend():
    backend = getattr(flask.g, '_mme_backend', None)
    if backend is None:
        backend = flask.g._mme_backend = DatastoreConnection()

    return backend
