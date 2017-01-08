"""
Module for accessing the backend connection
"""

from __future__ import with_statement, division, unicode_literals

import logging
import flask

from elasticsearch import Elasticsearch

from .managers import Managers

logger = logging.getLogger(__name__)



def get_backend():
    backend = getattr(flask.g, '_mme_backend', None)
    if backend is None:
        backend = flask.g._mme_backend = Managers(Elasticsearch())

    return backend
