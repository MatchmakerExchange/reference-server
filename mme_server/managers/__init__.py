"""
Module with the database connection and managers.
"""
from __future__ import with_statement, division, unicode_literals

import logging

from .patients import PatientManager
from .servers import ServerManager
from .vocabularies import VocabularyManager

logger = logging.getLogger(__name__)


class Managers:
    _managers = {}
    _db = None

    def __init__(self, backend):
        Managers._db = backend

    @classmethod
    def add_manager(cls, name, Manager):
        # name = Manager.NAME
        logger.debug('Registering manager: {} -> {}'.format(name, Manager))
        assert name, "Manager name is required"
        assert name not in cls._managers, "Manager name already registered: {}".format(name)
        cls._managers[name] = Manager

    @classmethod
    def get_manager(cls, name):
        return cls._managers[name](cls._db)


Managers.add_manager('patients', PatientManager)
Managers.add_manager('servers', ServerManager)
Managers.add_manager('vocabularies', VocabularyManager)
