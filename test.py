import json
import unittest
import os
import logging

from copy import deepcopy
from unittest import TestCase

import models
from elasticsearch import Elasticsearch
from datastore import DatastoreConnection
from schemas import validate_request, validate_response, ValidationError


EXAMPLE_REQUEST = {
    'patient': {
        'id': '1',
        'label': 'patient 1',
        'contact': {
            'name': 'First Last',
            'institution': 'Contact Institution',
            'href': 'mailto:first.last@example.com',
        },
        'features': [
            {
                'id': 'HP:0000252',
                'label': 'Microcephaly',
            },
            {
                'id': 'HP:0000522',
                'label': 'Alacrima',
            },
        ],
        'genomicFeatures': [{
            "gene": {
              "id": "EFTUD2"
            },
            "type": {
              "id": "SO:0001587",
              "label": "STOPGAIN"
            },
            "variant": {
              "alternateBases": "A",
              "assembly": "GRCh37",
              "end": 42929131,
              "referenceBases": "G",
              "referenceName": "17",
              "start": 42929130
            },
            "zygosity": 1
        }],
        'disorders': [{
            "id": "MIM:610536"
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
                        {'match': {'phenotype': 'HP:0000252'}},  # Microcephaly
                        {'match': {'phenotype': 'HP:0000522'}},  # Alacrima
                        {'match': {'phenotype': 'HP:0012639'}},  # Abnormal nervous system morphology
                        {'match': {'phenotype': 'HP:0100022'}},  # Movement abnormality
                        {'match': {'phenotype': 'HP:0002650'}},  # Scoliosis
                        {'match': {'gene': 'NGLY1'}},
                    ]
                }
            }
        }
        results = self.es.search(index='patients', body=query)
        hits = results['hits']['hits']
        self.assertEqual(hits[0]['_id'], 'P0001070')
        self.assertGreater(hits[0]['_score'], hits[1]['_score'])


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
        from server import app
        self.app = app
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

    def test_query_schema_extra_fields_must_start_with_underscore(self):
        self.request['patient']['_foo'] = 'bar'
        self.assertValidRequest(self.request)
        self.request['patient']['foo'] = 'bar'
        self.assertNotValidRequest(self.request)


class FlaskTests(unittest.TestCase):
    def setUp(self):
        from server import app

        self.client = app.test_client()
        self.data = json.dumps(EXAMPLE_REQUEST)
        self.accept_header = ('Accept', 'application/vnd.ga4gh.matchmaker.v1.0+json')
        self.content_type_header = ('Content-Type', 'application/json')
        self.headers = [self.accept_header, self.content_type_header]

    def assertValidResponse(self, data):
        validate_response(data)

    def test_with_headers(self):
        response = self.client.post('/match', data=self.data, headers=self.headers)
        self.assertEqual(response.status_code, 200)

    def test_response_content_type(self):
        response = self.client.post('/match', data=self.data, headers=self.headers)
        self.assertEqual(response.headers['Content-Type'], 'application/vnd.ga4gh.matchmaker.v1.0+json')

    def test_accept_header_required(self):
        headers = self.headers
        headers.remove(self.accept_header)
        response = self.client.post('/match', data=self.data, headers=headers)
        self.assertEqual(response.status_code, 406)

    def test_content_type_required(self):
        headers = self.headers
        headers.remove(self.content_type_header)
        response = self.client.post('/match', data=self.data, headers=headers)
        self.assertEqual(response.status_code, 415)

    def test_invalid_query(self):
        response = self.client.post('/match', data='{}', headers=self.headers)
        self.assertEqual(response.status_code, 422)
        self.assertTrue(json.loads(response.get_data(as_text=True))['message'])

    def test_response_schema(self):
        response = self.client.post('/match', data=self.data, headers=self.headers)
        self.assertValidResponse(json.loads(response.get_data(as_text=True)))


if __name__ == '__main__':
    unittest.main()
