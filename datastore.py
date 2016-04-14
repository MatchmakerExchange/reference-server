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
from parsers import OBOParser, GeneParser


logger = logging.getLogger(__name__)


class PatientManager:
    TYPE_NAME = 'patient'
    INDEX_CONFIG = {
        'mappings': {
            TYPE_NAME: {
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
        }
    }

    def __init__(self, index='patients', backend=None):
        if backend is None:
            backend = Elasticsearch()

        self._db = backend
        self._index = index

    def index(self, filename):
        """Populate the database with patient data from the given file"""
        from models import Patient

        with codecs.open(filename, encoding='utf-8') as ifp:
            data = json.load(ifp)

        for record in data:
            patient = Patient.from_api(record)
            self.index_patient(patient)

        # Update index before returning record count
        self._db.indices.refresh(index=self._index)
        n = self._db.count(index=self._index, doc_type=self.TYPE_NAME)
        logger.info('Datastore now contains {} patient records'.format(n['count']))

    def index_patient(self, patient):
        """Index the provided models.Patient object

        If a patient with the same id already exists in the index, the patient is replaced.
        """
        id = patient.get_id()
        data = patient.to_index()

        if not self._db.indices.exists(index=self._index):
            logger.info("Creating patient ElasticSearch index: {!r}".format(self._index))
            self._db.indices.create(index=self._index, body=self.INDEX_CONFIG)

        self._db.index(index=self._index, doc_type=self.TYPE_NAME, id=id, body=data)
        logger.info("Indexed patient: {!r}".format(id))

    def match(self, phenotypes, genes, n=5):
        """Return the n most similar patients, based on a list of phenotypes and candidate genes

        phenotypes - a list of HPO term IDs (including implied terms)
        genes - a list of ENSEMBL gene IDs for candidate genes
        """

        query_parts = []
        for id in phenotypes:
            query_parts.append({'match': {'phenotype': id}})

        for gene_id in genes:
            query_parts.append({'match': {'gene': gene_id}})

        query = {
            'query': {
                'bool': {
                    'should': [
                        query_parts
                    ]
                }
            }
        }

        result = self._db.search(index=self._index, body=query, size=n)
        return result['hits']['hits']


class VocabularyManager:
    TERM_TYPE_NAME = 'term'
    META_TYPE_NAME = 'meta'
    INDEX_CONFIG = {
        'mappings': {
            TERM_TYPE_NAME: {
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
        }
    }

    def __init__(self, backend=None):
        if backend is None:
            backend = Elasticsearch()

        self._db = backend

    def index(self, index, filename, Parser):
        """Index terms from the given file

        :param index: the id of the index
        :param filename: the path to the vocabulary file
        :param Parser: the Parser class to use to parse the vocabulary file
        """
        parser = Parser(filename)

        if self._db.indices.exists(index=index):
            logger.warning('Vocabulary index already exists: {!r}'.format(index))
        else:
            logger.info("Creating index: {!r}".format(index))
            self._db.indices.create(index=index, body=self.INDEX_CONFIG)

        logger.info("Parsing vocabulary from: {!r}".format(filename))
        commands = []
        for term in parser:
            id = term['id']
            command = [
                {'index': {'_id': id}},
                term,
            ]
            commands.extend(command)

        data = "".join([json.dumps(command) + "\n" for command in commands])
        self._db.bulk(data, index=index, doc_type=self.TERM_TYPE_NAME, refresh=True, request_timeout=60)

        n = self._db.count(index=index, doc_type=self.TERM_TYPE_NAME)
        logger.info('Index now contains {} terms'.format(n['count']))

    def index_hpo(self, filename):
        return self.index(index='hpo', filename=filename, Parser=OBOParser)

    def index_genes(self, filename):
        return self.index(index='genes', filename=filename, Parser=GeneParser)

    def get_term(self, id, index='_all'):
        """Get vocabulary term by ID"""
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'bool': {
                            'should': [
                                {'term': {'id': id}},
                                {'term': {'alt_id': id}},
                            ]
                        }
                    }
                }
            }
        }
        results = self._db.search(index=index, doc_type=self.TERM_TYPE_NAME, body=query)
        if results['hits']['total'] == 1:
            return results['hits']['hits'][0]['_source']
        else:
            logger.error("Unable to uniquely resolve term: {!r}".format(id))


class DatastoreConnection:
    def __init__(self):
        self._db = Elasticsearch()
        self.patients = PatientManager(backend=self._db)
        self.vocabularies = VocabularyManager(backend=self._db)
