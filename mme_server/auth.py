"""
Module hanlding the authentication of incoming and outgoing server requests.

Stores:
* Authenticated servers (`servers` index)
"""
from __future__ import with_statement, division, unicode_literals

import json
import logging
import flask

from functools import wraps

from flask import request, jsonify
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

from .datastore import ESIndex
from .models import get_backend


logger = logging.getLogger(__name__)


def get_server_manager():
    manager = getattr(flask.g, '_mme_servers', None)
    if manager is None:
        manager = flask.g._mme_servers = ServerManager()

    return manager


def auth_token_required():
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            logger.info("Authenticating request")
            token = request.headers.get('X-Auth-Token')
            auth = get_server_manager()
            server = auth.verify(token)
            if not server:
                error = jsonify(message='X-Auth-Token not authorized')
                error.status_code = 401
                return error

            flask.g.server = server
            return f(*args, **kwargs)
        return decorated_function
    return decorator


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
            # Refresh index to ensure immediately usable
            self.index.refresh()

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
        rows = []
        if self.index.exists():
            s = self.index.search()
            s = s.query('match_all')

            # Iterate through all, using scan
            for hit in s.scan():
                row = dict([(field, hit[field]) for field in self.FIELDS])
                rows.append(row)

        return {
            'fields': self.FIELDS,
            'rows': rows
        }

    def verify(self, key):
        if key and self.index.exists():
            s = self.index.search()
            s = s.filter('term', server_key=key)
            s = s.filter('term', direction='in')
            results = s.execute()

            if results.hits:
                return results.hits[0]
