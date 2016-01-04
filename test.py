import unittest
from unittest import TestCase

import api
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
    def setUp(self):
        self._request_template = {
            'patient': {
                'id': '1',
                'contact': {
                    'name': 'First Last',
                    'institution': 'Contact Institution',
                    'href': 'first.last@example.com',
                },
                'features': [],
                'genomicFeatures': [],
            }
        }

    def test_gene_symbol_match(self):
        data = self._request_template
        data['patient']['features'].extend([
            {
                'id': 'HP:0000252',
                'label': 'Microcephaly',
            },
            {
                'id': 'HP:0000522',
                'label': 'Alacrima',
            },
        ])
        data['patient']['genomicFeatures'].append({
            'gene': {
                'id': 'NGLY1'
            }
        })

        request = api.MatchRequest(data)
        response = api.match(request)

        self.assertTrue(isinstance(response, api.MatchResponse))
        results = response.to_json()['results']
        self.assertTrue(results)


if __name__ == '__main__':
    unittest.main()
