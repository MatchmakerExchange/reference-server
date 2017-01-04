"""
Module hanlding the authentication of incoming and outgoing server requests.
"""
from __future__ import with_statement, division, unicode_literals

import json
import logging

from elasticsearch_dsl import Search, Q

from ..base import BaseManager
from .parsers import OBOParser, GeneParser

logger = logging.getLogger(__name__)

HPO_DOC_TYPE = 'hpo'
GENE_DOC_TYPE = 'gene'

class VocabularyManager(BaseManager):
    NAME = 'vocabularies'
    DOC_TYPES = [HPO_DOC_TYPE, GENE_DOC_TYPE]
    TERM_CONFIG = {
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

    def get_config(self):
        # Create a separate doc_type for each ontology
        mappings = {}
        for doc_type in self.DOC_TYPES:
            mappings[doc_type] = self.TERM_CONFIG

        return {
            'mappings': mappings
        }

    def index_terms(self, doc_type, terms, **kwargs):
        commands = []
        for term in terms:
            id = term['id']
            command = [
                {'index': {'_index': self.get_name(), '_type': doc_type, '_id': id}},
                term,
            ]
            commands.extend(command)

        if commands:
            data = ''.join([json.dumps(command) + '\n' for command in commands])
            self.bulk(data, **kwargs)

    def iter_batches(self, iterator, batch_size):
        batch = []
        for item in iterator:
            batch.append(item)
            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def index_file(self, doc_type, filename, Parser, batch_size=1000):
        """Index terms from the given file

        :param doc_type: the doc_type for terms from this vocabulary
        :param filename: the path to the vocabulary file
        :param Parser: the Parser class to use to parse the vocabulary file
        """
        parser = Parser(filename)

        logger.info("Parsing vocabulary from: {!r}".format(filename))
        for batch in self.iter_batches(parser, batch_size=batch_size):
            self.index_terms(doc_type, batch, refresh=False)

    def index_hpo(self, filename, doc_type=HPO_DOC_TYPE):
        return self.index_file(doc_type=doc_type, filename=filename, Parser=OBOParser)

    def index_genes(self, filename, doc_type=GENE_DOC_TYPE):
        return self.index_file(doc_type=doc_type, filename=filename, Parser=GeneParser)

    def get_term(self, id):
        """Get vocabulary term by ID"""
        s = self.search()
        s = s.query(Q('term', id=id) | Q('term', alt_id=id))
        response = s.execute()

        if response.hits.total == 1:
            return response.hits[0].to_dict()
        else:
            logger.error("Unable to uniquely resolve term: {!r}".format(id))
