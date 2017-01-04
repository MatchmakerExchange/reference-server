"""
A base implementation of a manager and metaclass
"""
from __future__ import with_statement, division, unicode_literals

import logging

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

logger = logging.getLogger(__name__)


class BaseManager:
    def __init__(self, backend=None):
        self._db = backend

    def get_config(self):
        if hasattr(self, 'CONFIG'):
            return self.CONFIG
        else:
            raise NotImplementedError()

    def get_name(self):
        if hasattr(self, 'NAME'):
            return self.NAME
        else:
            raise NotImplementedError()

    def get_db(self):
        if hasattr(self, '_db'):
            return self._db
        else:
            raise NotImplementedError()

    def get_default_doc_type(self):
        if hasattr(self, 'DOC_TYPE'):
            return self.DOC_TYPE
        else:
            raise NotImplementedError()

    def create_index(self):
        logger.info("Creating ElasticSearch index: {!r}".format(self.get_name()))
        return self.get_db().indices.create(index=self.get_name(), body=self.get_config())

    def index_exists(self):
        return self.get_db().indices.exists(index=self.get_name())

    def ensure_index_exists(self):
        if not self.index_exists():
            return self.create_index()

    def search(self, **kwargs):
        if not self.index_exists():
            message = 'Index does not exist: {}'.format(self.get_name())
            logger.error(message)
            raise Exception(message)

        return Search(using=self.get_db(), index=self.get_name(), **kwargs)

    def save(self, doc, **kwargs):
        """Index a document

        Notable kwargs:
        id - if None, create a document, else update a document
        doc_type - if not provided, get_default_doc_type() is used
        """
        # Ensure the index exists
        self.ensure_index_exists()

        doc_type = kwargs.get('doc_type')
        if not doc_type:
            kwargs['doc_type'] = self.get_default_doc_type()
        kwargs.setdefault('index', self.get_name())
        return self.get_db().index(body=doc, **kwargs)

    def delete(self, id, **kwargs):
        """Delete a document

        Notable kwargs:
        doc_type - if not provided, get_default_doc_type() is used
        """
        if self.index_exists():
            doc_type = kwargs.get('doc_type')
            if not doc_type:
                kwargs['doc_type'] = self.get_default_doc_type()
            return self.get_db().delete(index=self.get_name(), id=id, **kwargs)

    def refresh(self, **kwargs):
        if self.index_exists():
            return self.get_db().indices.refresh(index=self.get_name(), **kwargs)

    def count(self, **kwargs):
        kwargs.setdefault('doc_type', self.get_default_doc_type())
        if self.index_exists():
            return self.get_db().count(index=self.get_name(), **kwargs)

    def bulk(self, data, refresh=True, request_timeout=60, **kwargs):
        # Ensure the index exists
        self.ensure_index_exists()
        self.get_db().bulk(data, index=self.get_name(), request_timeout=request_timeout, **kwargs)
        if refresh:
            self.refresh()

