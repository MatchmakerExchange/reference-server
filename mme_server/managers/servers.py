"""
Module hanlding the authentication of incoming and outgoing server requests.
"""
from __future__ import with_statement, division, unicode_literals

import json
import logging

from .base import BaseManager

logger = logging.getLogger(__name__)


class ServerManager(BaseManager):
    NAME = 'servers'
    SERVER_DOC_TYPE = 'server'
    CLIENT_DOC_TYPE = 'client'
    SERVER_DISPLAY_FIELDS = ['server_id', 'server_label', 'base_url']
    CLIENT_DISPLAY_FIELDS = ['server_id', 'server_label']
    CONFIG = {
        'mappings': {
            'server': {
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
                    'base_url': {
                        'type': 'string',
                        'index': 'not_analyzed',
                    }
                }
            },

            'client': {
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
                    }
                }
            }
        }
    }

    def add(self, server_id, server_label, server_key, direction, base_url=None):
        assert server_id and server_key and direction in ['in', 'out']

        if base_url and not base_url.startswith('https://'):
            logger.error('base URL must start with "https://"')
            return

        self.ensure_index_exists()

        doc_type = 'client' if direction == 'in' else 'server'

        # If it already exists, update
        s = self.search(doc_type=doc_type)
        s = s.filter('term', server_id=server_id)
        results = s.execute()

        if results.hits.total > 1:
            logger.error('Found two or more matching server entries')
        else:
            id = None
            if results.hits.total == 1:
                # Found a match, so update instead
                id = results.hits[0].meta.id

            data = {
                'server_id': server_id,
                'server_label': server_label,
                'server_key': server_key,
            }
            if doc_type == 'server':
                data['base_url'] = base_url,

            self.save(id=id, doc_type=doc_type, doc=data)
            logger.info("Authorized {}:\n{}".format(doc_type, json.dumps(data, indent=4, sort_keys=True)))
            # Refresh index to ensure immediately usable
            self.refresh()

    def remove(self, server_id, direction):
        if self.index_exists():
            doc_type = 'client' if direction == 'in' else 'server'
            s = self.search(doc_type=doc_type)
            s = s.filter('term', server_id=server_id)
            results = s.execute()

            for hit in results:
                id = hit.meta.id
                self.delete(id=id, doc_type=doc_type)
                logger.info("Deleted {}:{}".format(doc_type, hit.server_id))

    def list(self, direction):
        rows = []
        if self.index_exists():
            doc_type = 'client' if direction == 'in' else 'server'
            fields = self.CLIENT_DISPLAY_FIELDS if doc_type == 'client' else self.SERVER_DISPLAY_FIELDS
            s = self.search(doc_type=doc_type)
            s = s.query('match_all')

            # Iterate through all, using scan
            for hit in s.scan():
                row = dict([(field, hit[field]) for field in fields])
                rows.append(row)

        return {
            'fields': fields,
            'rows': rows
        }

    def verify(self, key):
        if key and self.index_exists():
            s = self.search()
            s = s.filter('term', server_key=key)
            results = s.execute()

            if results.hits:
                return results.hits[0]
