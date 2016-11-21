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


class ServerManager:
    TYPE_NAME = 'server'
    INDEX_CONFIG = {
        'mappings': {
            TYPE_NAME: {
                'properties': {
                    'server_id': {
                        'type': 'string',
                        'index': 'not_analyzed',
                    },
                    'server_label': {
                        'type': 'string',
                        'index': 'not_analyzed',
                    },
                    'server_key': {
                        'type': 'string',
                        'index': 'not_analyzed',
                    },
                    'direction': {
                        'type': 'string',
                        'index': 'not_analyzed',
                    },
                    'base_url': {
                        'type': 'string',
                        'index': 'not_analyzed',
                    }
                }
            }
        }
    }
    FIELDS = ['server_id', 'server_label', 'server_key', 'direction', 'base_url']

    def __init__(self, index='servers', backend=None):
        if backend is None:
            backend = Elasticsearch()

        self._db = backend
        self._index = index

    def add(self, server_id, server_label, server_key, direction, base_url):
        assert server_id and server_key and direction in ['in', 'out']

        if base_url and not base_url.startswith('https://'):
            logger.error('base URL must start with "https://"')
            return

        if not self._db.indices.exists(index=self._index):
            logger.info("Creating patient ElasticSearch index: {!r}".format(self._index))
            self._db.indices.create(index=self._index, body=self.INDEX_CONFIG)


        data = {
            'server_id': server_id,
            'server_label': server_label,
            'server_key': server_key,
            'direction': direction,
            'base_url': base_url,
        }

        # If it already exists, update
        s = Search(using=self._db, index=self._index, doc_type=self.TYPE_NAME)
        s = s.filter('term', server_id=server_id)
        s = s.filter('term', direction=direction)
        results = s.execute()
        if results.hits.total == 1:
            # Found a match, so update instead
            id = results.hits[0].meta.id
            self._db.index(index=self._index, doc_type=self.TYPE_NAME, id=id, body=data)
            logger.info("Updated authorization server:\n{}".format(json.dumps(data, indent=4, sort_keys=True)))
        elif results.hits.total:
            logger.error('Found two or more matching server entries')
        else:
            # Create a new server entry
            self._db.create(index=self._index, doc_type=self.TYPE_NAME, body=data)
            logger.info("Authorized server:\n{}".format(json.dumps(data, indent=4, sort_keys=True)))

    def remove(self, server_id, direction):
        if self._db.indices.exists(index=self._index):
            s = Search(using=self._db, index=self._index, doc_type=self.TYPE_NAME)
            s = s.filter('term', server_id=server_id)
            if direction:
                s = s.filter('term', direction=direction)
            results = s.execute()

            for hit in results:
                id = hit.meta.id
                self._db.delete(index=self._index, doc_type=self.TYPE_NAME, id=id)
                logger.info("Deleted server:{} direction:{}".format(hit.server_id, hit.direction))

    def list(self):
        if self._db.indices.exists(index=self._index):
            s = Search(using=self._db, index=self._index, doc_type=self.TYPE_NAME)
            s = s.query('match_all')
            response = s.execute()
            print('\t'.join(self.FIELDS))
            for hit in response:
                print('\t'.join([str(hit[field]) for field in self.FIELDS]))

    def verify(self, key):
        if key and self._db.indices.exists(index=self._index):
            s = Search(using=self._db, index=self._index, doc_type=self.TYPE_NAME)
            s = s.filter('term', server_key=key)
            s = s.filter('term', direction='in')
            results = s.execute()
            if results.hits:
                return results.hits[0]


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
        from .models import Patient

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

        s = Search(using=self._db, index=self._index)
        query = Q('bool', should=query_parts)
        s = s.query(query)
        s = s[:n]
        response = s.execute()
        return response


class VocabularyManager:
    TERM_TYPE_NAME = 'term'
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

        data = ''.join([json.dumps(command) + '\n' for command in commands])
        self._db.bulk(data, index=index, doc_type=self.TERM_TYPE_NAME, refresh=True, request_timeout=60)

        n = self._db.count(index=index, doc_type=self.TERM_TYPE_NAME)
        logger.info('Index now contains {} terms'.format(n['count']))

    def index_hpo(self, filename, index='hpo'):
        return self.index(index=index, filename=filename, Parser=OBOParser)

    def index_genes(self, filename, index='genes'):
        return self.index(index=index, filename=filename, Parser=GeneParser)

    def get_term(self, id, index='_all'):
        """Get vocabulary term by ID"""
        s = Search(using=self._db, index=index, doc_type=self.TERM_TYPE_NAME)
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
        self.servers = ServerManager(backend=self._db)
