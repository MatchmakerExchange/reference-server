import json
import unittest
import os
import logging

from tempfile import mkstemp
from unittest import TestCase

import models
from elasticsearch import Elasticsearch
from datastore import DatastoreConnection


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
        self.assertIn('small head', doc['synonym'])
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
                        {'match': {'gene': 'NGLY1'}},
                    ]
                }
            }
        }
        results = self.es.search(index='patients', body=query)
        self.assertEqual(results['hits']['hits'][0]['_id'], 'P0001070')


class DatastoreTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.backend = DatastoreConnection()

    def test_get_term(self):
        # Lookup term using alias
        term = self.backend.get_vocabulary_term('HP:0001366')

        self.assertEqual(term['id'], 'HP:0000252')
        self.assertEqual(term['name'], 'Microcephaly')
        self.assertEqual(len(term['is_a']), 2)
        self.assertAlmostEqual(len(term['term_category']), 20, delta=5)


class MatchRequestTests(TestCase):
    SCHEMA_FOLDER = 'schemas'
    SCHEMA_FILE = 'api.json'
    REQUEST_SCHEMA = '#/definitions/request'
    RESPONSE_SCHEMA = '#/definitions/response'

    @classmethod
    def setUpClass(cls):
        from jsonschema import RefResolver, FormatChecker
        with open(os.path.join(cls.SCHEMA_FOLDER, cls.SCHEMA_FILE)) as ifp:
            schema = json.load(ifp)

        cls.resolver = RefResolver.from_schema(schema)
        cls.format_checker = FormatChecker()
        cls.request_schema = cls.resolver.resolve_from_url(cls.REQUEST_SCHEMA)
        cls.response_schema = cls.resolver.resolve_from_url(cls.RESPONSE_SCHEMA)

    def setUp(self):
        self.request = {
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

    def assertValid(self, data, schema):
        from jsonschema import validate
        validate(data, schema, resolver=self.resolver, format_checker=self.format_checker)

    def assertNotValid(self, data, schema):
        from jsonschema import validate, ValidationError
        self.assertRaises(ValidationError, validate, data, schema, resolver=self.resolver, format_checker=self.format_checker)

    def test_query_schema_empty(self):
        self.request['patient'] = {}
        self.assertNotValid(self.request, self.request_schema)

    def test_query_schema_no_contact(self):
        self.request['patient']['contact'] = {}
        self.assertNotValid(self.request, self.request_schema)

    def test_query_schema_invalid_href_uri(self):
        self.request['patient']['contact']['href'] = 'first.last@example.com'
        self.assertNotValid(self.request, self.request_schema)

    def test_query_schema_no_id(self):
        self.request['patient'].pop('id')
        self.assertNotValid(self.request, self.request_schema)

    def test_query_schema_no_phenotype_or_genotype(self):
        self.request['patient']['features'] = []
        self.request['patient'].pop('genomicFeatures')
        self.assertNotValid(self.request, self.request_schema)

    def test_query_schema_complete(self):
        self.assertValid(self.request, self.request_schema)

    def test_query_schema_extra_fields_must_start_with_underscore(self):
        self.request['patient']['_foo'] = 'bar'
        self.assertValid(self.request, self.request_schema)
        self.request['patient']['foo'] = 'bar'
        self.assertNotValid(self.request, self.request_schema)

    def test_gene_symbol_match(self):
        self.request['patient']['features'] = [
            {
                'id': 'HP:0000252',
                'label': 'Microcephaly',
            },
            {
                'id': 'HP:0000522',
                'label': 'Alacrima',
            },
        ]
        self.request['patient']['genomicFeatures'] = [{
            'gene': {
                'id': 'NGLY1'
            }
        }]
        self.assertValid(self.request, self.request_schema)

        request_obj = models.MatchRequest(self.request)
        response_obj = models.match(request_obj)

        self.assertTrue(isinstance(response_obj, models.MatchResponse))
        response = response_obj.to_json()
        self.assertValid(response, self.response_schema)


class FlaskTests(unittest.TestCase):
    def setUp(self):
        from server import app
        self.app = app.test_client()
        self.data = '{"patient":{"id":"1","contact": {"name":"Jane Doe", "href":"mailto:jdoe@example.edu"},"features":[{"id":"HP:0000522"}],"genomicFeatures":[{"gene":{"id":"NGLY1"}}]}}'
        self.accept_header = ('Accept', 'application/vnd.ga4gh.matchmaker.v1.0+json')
        self.content_type_header = ('Content-Type', 'application/json')
        self.headers = [self.accept_header, self.content_type_header]

    def test_with_headers(self):
        response = self.app.post('/match', data=self.data, headers=self.headers)
        self.assertEqual(response.status_code, 200)

    def test_response_content_type(self):
        response = self.app.post('/match', data=self.data, headers=self.headers)
        self.assertEqual(response.headers['Content-Type'], 'application/vnd.ga4gh.matchmaker.v1.0+json')

    def test_accept_header_required(self):
        headers = self.headers
        headers.remove(self.accept_header)
        response = self.app.post('/match', data=self.data, headers=headers)
        self.assertEqual(response.status_code, 406)

    def test_content_type_required(self):
        headers = self.headers
        headers.remove(self.content_type_header)
        response = self.app.post('/match', data=self.data, headers=headers)
        self.assertEqual(response.status_code, 415)


if __name__ == '__main__':
    unittest.main()
