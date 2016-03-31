"""
Module for interfacing with the backend database (elasticsearch).

Stores:
* Patient records (`patients` index)
* Human Phenotype Ontology (HPO) (`hpo` index)
* Ensembl-Entrez-HGNC-GeneSymbol mappings (`genes` index)
"""

from __future__ import with_statement, division, unicode_literals

import sys
import os
import json
import logging

try:
    from urllib import urlretrieve
except ImportError:
    from urllib.request import urlretrieve

from elasticsearch import Elasticsearch
from parsers import OBOParser, GeneParser

logger = logging.getLogger(__name__)

# Matchmaker Exchange benchmark dataset of 50 patients
TEST_DATA_URL = 'https://raw.githubusercontent.com/ga4gh/mme-apis/hotfix/v1.0b/testing/benchmark_patients.json'
DEFAULT_DATA_FILENAME = 'data.json'

# Human Phenotype Ontology
HPO_URL = 'http://purl.obolibrary.org/obo/hp.obo'
DEFAULT_HPO_FILENAME = 'hp.obo'

# Gene identifier mappings, from HGNC: www.genenames.org
# Ensembl Genes 83 (GRCh38.p5)
# Columns:
#   HGNC ID
#   Approved Symbol
#   Approved Name
#   Synonyms
#   Entrez Gene ID (supplied by NCBI)
#   Ensembl Gene ID (supplied by Ensembl)
GENE_URL = 'http://www.genenames.org/cgi-bin/download?col=gd_hgnc_id&col=gd_app_sym&col=gd_app_name&col=gd_aliases&col=md_eg_id&col=md_ensembl_id&status=Approved&status_opt=2&where=&order_by=gd_app_sym_sort&format=text&limit=&hgnc_dbtag=on&submit=submit'
DEFAULT_GENE_FILENAME = 'genes.tsv'

# TODO: incorporate disease data from:
# http://www.orphadata.org/data/xml/en_product1.xml

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

    def __init__(self, backend, index='patients'):
        self._db = backend
        self._index = index

    def index(self, filename):
        """Populate the database with patient data from the given file"""
        from models import Patient

        if self._db.indices.exists(index=self._index):
            logging.warning("Patient index already exists: {!r}".format(self._index))
        else:
            logging.info("Creating patient ElasticSearch index: '{}'".format(self._index))
            self._db.indices.create(index=self._index, body=self.INDEX_CONFIG)

        with open(filename) as ifp:
            data = json.load(ifp)

        logging.info("Found data for {} patient records".format(len(data)))
        for record in data:
            patient = Patient(record)
            self.add_patient(patient)

        # Update index before returning record count
        self._db.indices.refresh(index=self._index)
        n = self._db.count(index=self._index, doc_type=self.TYPE_NAME)
        logger.info('Datastore now contains {} records'.format(n['count']))

    def add_patient(self, patient):
        """Add the provided api.Patient object to the datastore"""
        id = patient.id
        data = self._patient_to_index(patient)
        self._db.index(index=self._index, doc_type=self.TYPE_NAME, id=id, body=data)
        logging.info("Indexed patient: '{}'".format(id))

    def _patient_to_index(self, patient):
        genes = patient._get_genes()
        phenotypes = patient._get_implied_present_phenotypes()

        return {
            'phenotype': list(phenotypes),
            'gene': list(genes),
            'doc': patient.to_json(),
        }

    def find_similar_patients(self, patient, n=5):
        """Return the n most similar patients to the given query api.Patient"""
        from models import Patient

        query_parts = []
        for id in patient._get_implied_present_phenotypes():
            query_parts.append({'match': {'phenotype': id}})

        for gene_id in patient._get_genes():
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

        result = self._db.search(index=self._index, body=query)

        scored_patients = []
        for hit in result['hits']['hits'][:n]:
            # Just use the ElasticSearch TF/IDF score, normalized to [0, 1]
            score = 1 - 1 / (1 + hit['_score'])
            scored_patients.append((score, Patient(hit['_source']['doc'])))

        return scored_patients


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

    def __init__(self, backend):
        self._db = backend

    def index(self, index, filename, Parser):
        """Index terms from the given file

        :param index: the id of the index
        :param filename: the path to the vocabulary file
        :param Parser: the Parser class to use to parse the vocabulary file
        """
        parser = Parser(filename)

        if self._db.indices.exists(index=index):
            logging.warning('Vocabulary index already exists: {!r}'.format(index))
        else:
            logging.info("Creating index: {!r}".format(index))
            self._db.indices.create(index=index, body=self.INDEX_CONFIG)

        logging.info("Parsing vocabulary from: {!r}".format(filename))
        commands = []
        for term in parser:
            id = term['id']
            command = [
                {'index': {'_id': id}},
                term,
            ]
            commands.extend(command)

        data = "".join([json.dumps(command) + "\n" for command in commands])
        self._db.bulk(data, index=index, doc_type=self.TERM_TYPE_NAME, refresh=True)

        n = self._db.count(index=index, doc_type=self.TERM_TYPE_NAME)
        logger.info('Index now contains {} terms'.format(n['count']))

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
        self._es = Elasticsearch()
        self._patients = PatientManager(self)
        self._vocabularies = VocabularyManager(self)

    def index_patients(self, filename):
        return self._patients.index(filename)

    def index_hpo(self, filename):
        return self._vocabularies.index(index='hpo', filename=filename, Parser=OBOParser)

    def index_genes(self, filename):
        return self._vocabularies.index(index='genes', filename=filename, Parser=GeneParser)

    def get_vocabulary_term(self, id, index='_all'):
        return self._vocabularies.get_term(id, index=index)

    def find_similar_patients(self, patient, n=5):
        """Return the n most similar patients to the given query api.Patient"""
        return self._patients.find_similar_patients(patient=patient, n=n)

    def search(self, *args, **kwargs):
        """Expose ElasticSearch method"""
        return self._es.search(*args, **kwargs)

    def bulk(self, *args, **kwargs):
        """Expose ElasticSearch method"""
        return self._es.bulk(*args, **kwargs)

    def index(self, *args, **kwargs):
        """Expose ElasticSearch method"""
        return self._es.index(*args, **kwargs)

    def count(self, *args, **kwargs):
        """Expose ElasticSearch method"""
        return self._es.count(*args, **kwargs)

    @property
    def indices(self):
        """Expose ElasticSearch property"""
        return self._es.indices


def initialize_backend(data_filename, hpo_filename, gene_filename):
    backend = DatastoreConnection()
    backend.index_hpo(hpo_filename)
    backend.index_genes(gene_filename)
    # Patients must be indexed AFTER vocabularies
    backend.index_patients(data_filename)


def fetch_resource(url, filename):
    if os.path.isfile(filename):
        logger.info('Found local resource: {}'.format(filename))
    else:
        logger.info('Downloading file from: {}'.format(url))
        urlretrieve(url, filename)
        logger.info('Saved file to: {}'.format(filename))


def parse_args(args):
    from argparse import ArgumentParser

    description = "Initialize datastore with example data and necessary vocabularies"

    parser = ArgumentParser(description=description)
    parser.add_argument("--data-file", default=DEFAULT_DATA_FILENAME,
                        dest="data_filename", metavar="FILE",
                        help="Load data from the following file (will download test data if file does not exist)")
    parser.add_argument("--hpo-file", default=DEFAULT_HPO_FILENAME,
                        dest="hpo_filename", metavar="FILE",
                        help="Load HPO from the following file (will download if file does not exist)")
    parser.add_argument("--gene-file", default=DEFAULT_GENE_FILENAME,
                        dest="gene_filename", metavar="FILE",
                        help="Load gene mappings from the following file (will download if file does not exist)")

    return parser.parse_args(args)


def main(args=sys.argv[1:]):
    args = parse_args(args)
    logging.basicConfig(level='INFO')

    fetch_resource(TEST_DATA_URL, args.data_filename)
    fetch_resource(HPO_URL, args.hpo_filename)
    fetch_resource(GENE_URL, args.gene_filename)

    initialize_backend(args.data_filename, args.hpo_filename, args.gene_filename)


if __name__ == '__main__':
    sys.exit(main())
