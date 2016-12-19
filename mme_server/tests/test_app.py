import os
import json
import unittest

from copy import deepcopy
from unittest import TestCase
from random import randint

from elasticsearch import Elasticsearch

from mme_server.datastore import DatastoreConnection
from mme_server.schemas import validate_request, validate_response, ValidationError

EXAMPLE_REQUEST = {
    'patient': {
        'id': '1',
        'label': 'patient 1',
        'contact': {
            'name': 'First Last',
            'institution': 'Contact Institution',
            'href': 'mailto:first.last@example.com',
        },
        'ageOfOnset': 'HP:0003577',
        'inheritanceMode': 'HP:0000006',
        'features': [
            {
                'id': 'HP:0000252',
                'label': 'Microcephaly',
            },
            {
                'id': 'HP:0000522',
                'label': 'Alacrima',
                'ageOfOnset': 'HP:0003593',
            },
        ],
        'genomicFeatures': [{
            "gene": {
              "id": "EFTUD2",
            },
            "type": {
              "id": "SO:0001587",
              "label": "STOPGAIN",
            },
            "variant": {
              "alternateBases": "A",
              "assembly": "GRCh37",
              "end": 42929131,
              "referenceBases": "G",
              "referenceName": "17",
              "start": 42929130,
            },
            "zygosity": 1,
        }],
        'disorders': [{
            "id": "MIM:610536",
        }],
    }
}


class ElasticSearchTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.es = Elasticsearch()

    # Unittest test backwards compatibility to Python 2.X
    try:
        assertCountEqual = TestCase.assertCountEqual
    except AttributeError:
        assertCountEqual = TestCase.assertItemsEqual

    def test_patient_indexed(self):
        record = self.es.get(index='patients', id='P0001135')
        self.assertTrue(record['found'])
        self.assertCountEqual(record['_source']['gene'], ['ENSG00000151092'])  # NGLY1

    def test_hpo_indexed(self):
        term = self.es.get(index='hpo', id='HP:0000252')
        self.assertTrue(term['found'])
        doc = term['_source']
        self.assertEqual(doc['name'], 'Microcephaly')
        self.assertAlmostEqual(len(doc['alt_id']), 4, delta=1)
        self.assertIn('small head', [term.lower() for term in doc['synonym']])
        self.assertCountEqual(doc['is_a'], ['HP:0040195', 'HP:0007364'])
        self.assertAlmostEqual(len(doc['term_category']), 19, delta=2)

    def test_gene_filter(self):
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'term': {
                            'gene': 'ENSG00000151092',  # NGLY1
                        }
                    }
                }
            }
        }
        results = self.es.search(index='patients', body=query)
        self.assertEqual(results['hits']['total'], 8, "Expected 8 cases with NGLY1 gene")

    def test_phenotype_filter(self):
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'term': {
                            'phenotype': 'HP:0000118'
                        }
                    }
                }
            }
        }
        results = self.es.search(index='patients', body=query)
        self.assertEqual(results['hits']['total'], 50, "Expected 50 cases with some phenotypic abnormality")

    def test_fuzzy_search(self):
        query = {
            'query': {
                'bool': {
                    'should': [
                        {'match': {'phenotype': 'HP:0001250'}},  # Seizures
                        {'match': {'phenotype': 'HP:0006852'}},  # Eposodic hypotonia
                        {'match': {'phenotype': 'HP:0011675'}},  # Arrhythmia
                        {'match': {'phenotype': 'HP:0003312'}},  # Abnormal vertebra
                        {'match': {'gene': 'GPX4'}},
                    ]
                }
            }
        }
        results = self.es.search(index='patients', body=query)
        hits = results['hits']['hits']
        # Most similar patient from test dataset
        self.assertEqual(hits[0]['_id'], 'P0001059')


class DatastoreTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.backend = DatastoreConnection()

    def test_get_term(self):
        # Lookup term using alias
        term = self.backend.vocabularies.get_term(id='HP:0001366')

        self.assertEqual(term['id'], 'HP:0000252')
        self.assertEqual(term['name'], 'Microcephaly')
        self.assertEqual(len(term['is_a']), 2)
        self.assertAlmostEqual(len(term['term_category']), 20, delta=5)


class MatchRequestTests(TestCase):
    def setUp(self):
        self.request = deepcopy(EXAMPLE_REQUEST)

    def assertValidRequest(self, data):
        validate_request(data)

    def assertNotValidRequest(self, data):
        self.assertRaises(ValidationError, validate_request, data)

    def test_query_schema_empty(self):
        self.request['patient'] = {}
        self.assertNotValidRequest(self.request)

    def test_query_schema_no_contact(self):
        self.request['patient']['contact'] = {}
        self.assertNotValidRequest(self.request)

    def test_query_schema_invalid_href_uri(self):
        self.request['patient']['contact']['href'] = 'first.last@example.com'
        self.assertNotValidRequest(self.request)

    def test_query_schema_no_id(self):
        self.request['patient'].pop('id')
        self.assertNotValidRequest(self.request)

    def test_query_schema_no_phenotype_or_genotype(self):
        self.request['patient']['features'] = []
        self.request['patient'].pop('genomicFeatures')
        self.assertNotValidRequest(self.request)

    def test_query_schema_complete(self):
        self.assertValidRequest(self.request)

    def test_query_schema_extra_fields_allowed(self):
        self.request['patient']['_foo'] = 'bar'
        self.assertValidRequest(self.request)
        self.request['patient']['foo'] = 'bar'
        self.assertValidRequest(self.request)


class FlaskTests(unittest.TestCase):
    def setUp(self):
        from mme_server.server import app
        from mme_server.cli import add_server

        self.client = app.test_client()
        self.data = json.dumps(EXAMPLE_REQUEST)
        self.auth_token = 'mysecretauthtoken'
        self.test_server_id = 'test_server_{}'.format(randint(0, 1000000))
        add_server(self.test_server_id, 'in', key=self.auth_token)

        self.accept_header = ('Accept', 'application/vnd.ga4gh.matchmaker.v1.0+json')
        self.content_type_header = ('Content-Type', 'application/json')
        self.auth_token_header = ('X-Auth-Token', self.auth_token)
        self.headers = [
            self.accept_header,
            self.content_type_header,
            self.auth_token_header,
        ]

    def tearDown(self):
        from mme_server.cli import remove_server
        remove_server(self.test_server_id, 'in')

    def assertValidResponse(self, data):
        validate_response(data)

    def test_match_request(self):
        response = self.client.post('/v1/match', data=self.data, headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/vnd.ga4gh.matchmaker.v1.0+json')
        self.assertValidResponse(json.loads(response.get_data(as_text=True)))

    def test_accept_header_required(self):
        headers = self.headers
        headers.remove(self.accept_header)
        response = self.client.post('/v1/match', data=self.data, headers=headers)
        self.assertEqual(response.status_code, 406)

    def test_content_type_required(self):
        headers = self.headers
        headers.remove(self.content_type_header)
        response = self.client.post('/v1/match', data=self.data, headers=headers)
        self.assertEqual(response.status_code, 415)

    def test_invalid_query(self):
        response = self.client.post('/v1/match', data='{}', headers=self.headers)
        self.assertEqual(response.status_code, 422)
        self.assertTrue(json.loads(response.get_data(as_text=True))['message'])


class EndToEndTests(unittest.TestCase):
    def setUp(self):
        from mme_server.cli import add_server
        self.auth_token = 'mysecretauthtoken'
        self.test_server_id = 'test_server_{}'.format(randint(0, 1000000))
        add_server(self.test_server_id, 'in', key=self.auth_token)

        self.accept_header = ('Accept', 'application/vnd.ga4gh.matchmaker.v1.0+json')
        self.content_type_header = ('Content-Type', 'application/json')
        self.auth_token_header = ('X-Auth-Token', self.auth_token)
        self.headers = [
            self.accept_header,
            self.content_type_header,
            self.auth_token_header,
        ]

    def tearDown(self):
        from mme_server.cli import remove_server
        remove_server(self.test_server_id, 'in')

    def test_query(self):
        from mme_server.server import app
        self.client = app.test_client()
        self.data = json.dumps(EXAMPLE_REQUEST)
        response = self.client.post('/v1/match', data=self.data, headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'application/vnd.ga4gh.matchmaker.v1.0+json')
        response_data = json.loads(response.get_data(as_text=True))
        validate_response(response_data)
        self.assertEqual(len(response_data['results']), 5)

    @unittest.skipUnless('MME_TEST_QUICKSTART' in os.environ, 'Not testing quickstart data loading')
    def test_quickstart(self):
        from mme_server import main
        # Index all data
        main(['quickstart'])
        self.test_query()


if __name__ == '__main__':
    unittest.main()
