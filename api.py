"""
The API module:

Contains API methods and classes for API objects.
Handles parsing of API requests into API objects, and serializing API objects into API responses.

Also contains some code to help convert API objects to their database representations.
"""
from __future__ import with_statement, division, unicode_literals

import json

from datastore import DatastoreConnection


class Feature:
    # Connection to backend to validate vocabulary terms
    db = DatastoreConnection()

    def __init__(self, data):
        self._observed = data.get('observed', 'yes') == 'yes'
        # TODO: parse ageOfOnset
        self.term = self.db.get_vocabulary_term(data['id'])

    def _get_implied_terms(self):
        return self.term['term_category']

    def _get_id(self):
        return self.term['id']

    @property
    def observed(self):
        return self._observed


class GenomicFeature:
    # Connection to backend to validate vocabulary terms
    db = DatastoreConnection()

    def __init__(self, data):
        self.term = None
        gene_id = data.get('gene', {}).get('id')
        # TODO: parse additional genomicFeature fields
        if gene_id:
            self.term = self.db.get_vocabulary_term(gene_id)

    def _get_gene_id(self):
        if self.term:
            return self.term['id']


class Patient:
    def __init__(self, data):
        self.id = data['id']
        self.contact = data['contact']
        assert self.contact['name'] and self.contact['href']

        features_json = data.get('features', [])
        genomic_features_json = data.get('genomicFeatures', [])

        assert features_json or genomic_features_json, "At least one of 'features' or 'genomicFeatures' must be provided"

        # Parse phenotype terms
        features = [Feature(feature_json) for feature_json in features_json]

        # Parse genomic features
        genomic_features = [GenomicFeature(gf_json) for gf_json in genomic_features_json]

        assert features or genomic_features, "Was unable to parse any phenotype or gene terms"

        disorders = data.get('disorders', [])
        self.label = data.get('label')
        self.age_of_onset = data.get('ageOfOnset')
        self.features = features
        self.genomic_features = genomic_features
        self.disorders = disorders
        self.test = data.get('test', False)

    def _get_genes(self):
        genes = set()
        for genomic_feature in self.genomic_features:
            gene_id = genomic_feature._get_gene_id()
            if gene_id:
                genes.add(gene_id)

        return genes

    def _get_present_phenotypes(self):
        terms = set()
        for feature in self.features:
            if feature.observed:
                terms.add(feature._get_id())

        return terms

    def _get_implied_present_phenotypes(self):
        terms = set()
        for feature in self.features:
            if feature.observed:
                terms.update(feature._get_implied_terms())

        return terms

    def to_json(self):
        data = {
            'id': self.id,
            'contact': {
                'name': self.contact['name'],
                'href': self.contact['href'],
            }
        }

        if self.label:
            data['label'] = self.label

        if self.age_of_onset:
            data['ageOfOnset'] = self.age_of_onset

        phenotype_ids = self._get_present_phenotypes()
        if phenotype_ids:
            data['features'] = [{'id': id} for id in phenotype_ids]

        gene_ids = self._get_genes()
        if gene_ids:
            data['genomicFeatures'] = [{'gene': {'id': gene_id}} for gene_id in gene_ids]

        if self.disorders:
            data['disorders'] = self.disorders

        if self.test:
            data['test'] = True

        return data


class MatchRequest:
    def __init__(self, request):
        self.patient = Patient(request['patient'])
        self._data = request


class MatchResult:
    def __init__(self, match, score):
        self.match = match
        self.score = score

    def to_json(self):
        response = {}
        response['score'] = {'patient': self.score}
        response['patient'] = self.match.to_json()
        return response


def match(request, backend=None):
    assert isinstance(request, MatchRequest), "Argument to match must be MatchResponse object"

    if not backend:
        backend = DatastoreConnection()

    matches = []
    # Unpack patient and query backend
    patient = request.patient
    for score, patient in backend.find_similar_patients(patient):
        match = MatchResult(patient, score)
        matches.append(match)

    response = MatchResponse(matches)
    return response


class MatchResponse:
    def __init__(self, response):
        self._data = response

    def to_json(self):
        response = {}
        response['results'] = [match.to_json() for match in self._data]
        return response
