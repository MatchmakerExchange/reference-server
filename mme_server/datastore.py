"""
Module for interfacing with the backend database (elasticsearch).

Stores:
* Patient records (`patients` index)
* Human Phenotype Ontology (HPO) (`hpo` index)
* Ensembl-Entrez-HGNC-GeneSymbol mappings (`genes` index)
"""

from __future__ import with_statement, division, unicode_literals

import json
import logging
import codecs

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

from .parsers import OBOParser, GeneParser


logger = logging.getLogger(__name__)


class ESIndex:
    def __init__(self, db, name, doc_type, doc_config):
        self._db = db
        self._name = name
        self._doc_type = doc_type
        self._doc_config = doc_config

    def _get_config(self):
        return {
            'mappings': {
                self._doc_type: self._doc_config
            }
        }

    def create(self):
        logger.info("Creating ElasticSearch index: {!r}".format(self._name))
        return self._db.indices.create(index=self._name, body=self._get_config())

    def exists(self):
        return self._db.indices.exists(index=self._name)

    def ensure_exists(self):
        if not self.exists():
            return self.create()

    def search(self):
        if self.exists():
            return Search(using=self._db, index=self._name, doc_type=self._doc_type)

    def save(self, doc, id=None):
        """Create a new document if id is None, else update"""
        # Ensure the index exists
        self.ensure_exists()

        if id is None:
            return self._db.create(index=self._name, doc_type=self._doc_type, body=doc)
        else:
            return self._db.index(index=self._name, doc_type=self._doc_type, id=id, body=doc)

    def delete(self, id, index=None):
        if self.exists():
            return self._db.delete(index=self._name, doc_type=self._doc_type, id=id)

    def refresh(self, **kwargs):
        if self.exists():
            return self._db.indices.refresh(index=self._name, **kwargs)

    def count(self):
        if self.exists():
            return self._db.count(index=self._name, doc_type=self._doc_type)

    def bulk(self, data, refresh=True, request_timeout=60, **kwargs):
        # Ensure the index exists
        self.ensure_exists()
        self._db.bulk(data, index=self._name, doc_type=self._doc_type, request_timeout=request_timeout, **kwargs)
        if refresh:
            self.refresh()


class PatientManager:
    DOC_CONFIG = {
        '_all': {
            'enabled': False,
        },
        'properties': {
            'phenotype': {
                'type': 'string',
                'index': 'not_analyzed',
            },
            'gene': {
                'type': 'string',
                'index': 'not_analyzed',
            },
            'doc': {
                'type': 'object',
                'enabled': False,
                'include_in_all': False,
            }
        }
    }

    def __init__(self, backend=None):
        if backend is None:
            backend = Elasticsearch()

        self._index = ESIndex(db=backend,
                              name='patients',
                              doc_type='patient',
                              doc_config=self.DOC_CONFIG)

    @property
    def index(self):
        return self._index

    def index_file(self, filename):
        """Populate the database with patient data from the given file"""
        from .models import Patient

        with codecs.open(filename, encoding='utf-8') as ifp:
            data = json.load(ifp)

        for record in data:
            patient = Patient.from_api(record)
            self.index_patient(patient)

        # Update index before returning record count
        self.index.refresh()
        n = self.index.count()
        logger.info('Datastore now contains {} patient records'.format(n))

    def index_patient(self, patient):
        """Index the provided models.Patient object

        If a patient with the same id already exists in the index, the patient is replaced.
        """
        id = patient.get_id()
        data = patient.to_index()

        self.index.save(id=id, doc=data)
        logger.info("Indexed patient: {!r}".format(id))

    def match(self, phenotypes, genes, n=10):
        """Return an elasticsearch_dsl.Response of the most similar patients to a list of phenotypes and candidate genes

        phenotypes - a list of HPO term IDs (including implied terms)
        genes - a list of ENSEMBL gene IDs for candidate genes
        """

        query_parts = []
        for id in phenotypes:
            query_parts.append(Q('match', phenotype=id))

        for gene_id in genes:
            query_parts.append(Q('match', gene=gene_id))

        query = Q('bool', should=query_parts)
        s = self.index.search()
        s = s.query(query)[:n]
        response = s.execute()
        return response


class VocabularyManager:
    DOC_TYPE = 'term'
    DOC_CONFIG = {
        '_all': {
            'enabled': False,
        },
        'properties': {
            'id': {
                'type': 'string',
                'index': 'not_analyzed',
            },
            'name': {
                'type': 'string',
            },
            'synonym': {
                'type': 'string',
            },
            'alt_id': {
                'type': 'string',
                'index': 'not_analyzed',
            },
            'is_a': {
                'type': 'string',
                'index': 'not_analyzed',
            },
            'term_category': {
                'type': 'string',
                'index': 'not_analyzed',
            },
        }
    }

    def __init__(self, backend=None):
        if backend is None:
            backend = Elasticsearch()

        self._db = backend
        self._indices = {}

    def get_index(self, index_name):
        index = self._indices.get(index_name)
        if index is None:
            index = ESIndex(db=self._db,
                            name=index_name,
                            doc_type=self.DOC_TYPE,
                            doc_config=self.DOC_CONFIG)

        return index

    def index_terms(self, index, terms, **kwargs):
        commands = []
        for term in terms:
            id = term['id']
            command = [
                {'index': {'_id': id}},
                term,
            ]
            commands.extend(command)

        if commands:
            data = ''.join([json.dumps(command) + '\n' for command in commands])
            index.bulk(data, **kwargs)

    def iter_batches(self, iterator, batch_size):
        batch = []
        for item in iterator:
            batch.append(item)
            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def index_file(self, index_name, filename, Parser, batch_size=1000):
        """Index terms from the given file

        :param index_name: the name of the index
        :param filename: the path to the vocabulary file
        :param Parser: the Parser class to use to parse the vocabulary file
        """
        parser = Parser(filename)

        index = self.get_index(index_name)
        logger.info("Parsing vocabulary from: {!r}".format(filename))
        for batch in self.iter_batches(parser, batch_size=batch_size):
            self.index_terms(index, batch, refresh=False)

    def index_hpo(self, filename, index='hpo'):
        return self.index_file(index_name=index, filename=filename, Parser=OBOParser)

    def index_genes(self, filename, index='genes'):
        return self.index_file(index_name=index, filename=filename, Parser=GeneParser)

    def get_term(self, id, index='_all'):
        """Get vocabulary term by ID"""
        s = Search(using=self._db, index=index, doc_type=self.DOC_TYPE)
        s = s.query(Q('term', id=id) | Q('term', alt_id=id))
        response = s.execute()

        if response.hits.total == 1:
            return response.hits[0].to_dict()
        else:
            logger.error("Unable to uniquely resolve term: {!r}".format(id))


class DatastoreConnection:
    def __init__(self):
        self._db = Elasticsearch()
        self.patients = PatientManager(backend=self._db)
        self.vocabularies = VocabularyManager(backend=self._db)
