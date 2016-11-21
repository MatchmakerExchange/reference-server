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
            self.create()

    def search(self):
        return Search(using=self._db, index=self._name, doc_type=self._doc_type)

    def save(self, doc, id=None):
        """Create a new document if id is None, else update"""
        # Ensure the index exists
        self.ensure_exists()

        if id is None:
            self._db.create(index=self._name, doc_type=self._doc_type, body=doc)
        else:
            self._db.index(index=self._name, doc_type=self._doc_type, id=id, body=doc)

    def delete(self, id, index=None):
        if self.exists():
            self._db.delete(index=self._name, doc_type=self._doc_type, id=id)

    def refresh(self):
        self._db.indices.refresh(index=self._name)

    def count(self):
        self._db.count(index=self._name, doc_type=self._doc_type)

    def bulk(self, data, refresh=True, request_timeout=60, **args):
        self._db.bulk(data, index=self._name, doc_type=self._doc_type)


class ServerManager:
    FIELDS = ['server_id', 'server_label', 'server_key', 'direction', 'base_url']
    DOC_CONFIG = {
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

    def __init__(self, backend=None):
        if backend is None:
            backend = Elasticsearch()

        self._index = ESIndex(db=backend,
                              name='servers',
                              doc_type='server',
                              doc_config=self.DOC_CONFIG)

    @property
    def index(self):
        return self._index

    def add(self, server_id, server_label, server_key, direction, base_url):
        assert server_id and server_key and direction in ['in', 'out']

        if base_url and not base_url.startswith('https://'):
            logger.error('base URL must start with "https://"')
            return

        self.index.ensure_exists()

        data = {
            'server_id': server_id,
            'server_label': server_label,
            'server_key': server_key,
            'direction': direction,
            'base_url': base_url,
        }

        # If it already exists, update
        s = self.index.search()
        s = s.filter('term', server_id=server_id)
        s = s.filter('term', direction=direction)
        results = s.execute()

        if results.hits.total > 1:
            logger.error('Found two or more matching server entries')
        else:
            id = None
            if results.hits.total == 1:
                # Found a match, so update instead
                id = results.hits[0].meta.id

            self.index.save(id=id, doc=data)
            logger.info("Authorized server:\n{}".format(json.dumps(data, indent=4, sort_keys=True)))

    def remove(self, server_id, direction):
        if self.index.exists():
            s = self.index.search()
            s = s.filter('term', server_id=server_id)

            if direction:
                s = s.filter('term', direction=direction)
            results = s.execute()

            for hit in results:
                id = hit.meta.id
                self.index.delete(id=id)
                logger.info("Deleted server:{} direction:{}".format(hit.server_id, hit.direction))

    def list(self):
        if self.index.exists():
            s = self.index.search()
            s = s.query('match_all')
            response = s.execute()

            print('\t'.join(self.FIELDS))
            for hit in response:
                print('\t'.join([str(hit[field]) for field in self.FIELDS]))

    def verify(self, key):
        if key and self.index.exists():
            s = self.index.search()
            s = s.filter('term', server_key=key)
            s = s.filter('term', direction='in')
            results = s.execute()

            if results.hits:
                return results.hits[0]


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
        logger.info('Datastore now contains {} patient records'.format(n['count']))

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

    def get_index(index):
        index = self._indices.get(index)
        if index is None:
            index = ESIndex(db=self._db,
                            name=index,
                            doc_type=self.DOC_TYPE,
                            doc_config=self.DOC_CONFIG)

            index.create()

        return index

    def index_file(self, index, filename, Parser):
        """Index terms from the given file

        :param index: the name of the index
        :param filename: the path to the vocabulary file
        :param Parser: the Parser class to use to parse the vocabulary file
        """
        parser = Parser(filename)

        index = self.get_index(index)
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

        index.bulk(data)

        n = index.count()
        logger.info('Index now contains {} terms'.format(n['count']))

    def index_hpo(self, filename, index='hpo'):
        return self.index_file(index=index, filename=filename, Parser=OBOParser)

    def index_genes(self, filename, index='genes'):
        return self.index_file(index=index, filename=filename, Parser=GeneParser)

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
        self.servers = ServerManager(backend=self._db)
