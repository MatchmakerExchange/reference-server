"""
Module for providing file and dataset parsing functionality.
"""

from __future__ import with_statement, division, unicode_literals

import codecs

from csv import DictReader
from collections import defaultdict

from .obo import Parser as BaseOBOParser


class BaseParser:
    def __init__(self, filename):
        self._filename = filename

    def documents(self):
        raise NotImplementedError()

    def __iter__(self):
        return self.documents()


class OBOParser(BaseParser):
    def documents(self):
        parser = BaseOBOParser(codecs.open(self._filename, encoding='utf-8'))

        # Parse all terms first
        terms = {}

        def get_tag_strings(stanza, tag):
            return list(map(str, stanza.tags.get(tag, [])))

        for stanza in parser:
            id = str(stanza.tags['id'][0])
            name = get_tag_strings(stanza, 'name')
            alt_id = get_tag_strings(stanza, 'alt_id')
            synonym = get_tag_strings(stanza, 'synonym')
            is_a = get_tag_strings(stanza, 'is_a')
            is_obsolete = get_tag_strings(stanza, 'is_obsolete')
            # Skip obsolete terms
            if is_obsolete and 'true' in is_obsolete:
                continue

            terms[id] = {
                'id': id,
                'name': name,
                'synonym': synonym,
                'alt_id': alt_id,
                'is_a': is_a,
                'term_category': [],  # Added later
            }

        # Then compute ancestor paths for each term
        def get_ancestors(node_id, ancestors=None):
            if ancestors is None:
                ancestors = set()
            ancestors.add(node_id)
            for parent_id in terms[node_id]['is_a']:
                get_ancestors(parent_id, ancestors)
            return ancestors

        for id in terms:
            term = terms[id]
            term['term_category'] = list(get_ancestors(id))

            yield term


class TSVParser(BaseParser):
    def _documents(self, columns):
        with codecs.open(self._filename, encoding='utf-8') as ifp:
            reader = DictReader(ifp, delimiter=str('\t'))
            for row in reader:
                term = defaultdict(list)
                for column in columns:
                    key = column['column']
                    field = column['field']
                    prefix = column.get('prefix')
                    delimiter = column.get('delimiter')
                    length = column.get('length')

                    value = row[key]
                    # Split multivalued fields
                    if delimiter:
                        values = value.split(delimiter)
                    else:
                        values = [value]

                    # Ensure all lengths are correct
                    if length:
                        for value in values:
                            assert len(value) == length or not value

                    # Prepend prefix to all values
                    if prefix:
                        values = ['{}:{}'.format(prefix, value) for value in values]

                    term[field].extend(values)

                # ElasticSearch does not allow id field to be singleton list
                assert len(term['id']) == 1
                term['id'] = term['id'][0]
                yield term


class GeneParser(TSVParser):
    def documents(self):
        columns = [
            {
                'column': 'Ensembl ID(supplied by Ensembl)',
                'field': 'id',
                'length': 15,
            },
            {
                'column': 'Approved Name',
                'field': 'name',
            },
            {
                'column': 'Approved Symbol',
                'field': 'alt_id',
            },
            {
                'column': 'Previous Symbols',
                'field': 'alt_id',
                'delimiter': ', ',
            },
            {
                'column': 'Synonyms',
                'field': 'alt_id',
                'delimiter': ', ',
            },
            {
                'column': 'Entrez Gene ID(supplied by NCBI)',
                'field': 'alt_id',
                'prefix': 'NCBIGene',
            },
            {
                'column': 'HGNC ID',
                'field': 'alt_id',
            },
        ]
        return TSVParser._documents(self, columns)
