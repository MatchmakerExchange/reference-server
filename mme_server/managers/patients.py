"""
Module hanlding the authentication of incoming and outgoing server requests.
"""
from __future__ import with_statement, division, unicode_literals

import json
import logging
import codecs

from elasticsearch_dsl import Q

from .base import BaseManager

logger = logging.getLogger(__name__)


class PatientManager(BaseManager):
    NAME = 'patients'
    DOC_TYPE = 'patient'
    CONFIG = {
        'mappings': {
            'patient': {
                '_all': {
                    'enabled': False,
                },
                'properties': {
                    'phenotype': {
                        'type': 'string',
                        'index': 'not_analyzed',
                    },
                    'gene': {
                        'type': 'string',
                        'index': 'not_analyzed',
                    },
                    'doc': {
                        'type': 'object',
                        'enabled': False,
                        'include_in_all': False,
                    }
                }
            }
        }
    }

    def index_file(self, filename):
        """Populate the database with patient data from the given file"""
        # Import within function to avoid cyclic import
        from ..models import Patient

        with codecs.open(filename, encoding='utf-8') as ifp:
            data = json.load(ifp)

        for record in data:
            patient = Patient.from_api(record)
            self.index_patient(patient)

        # Update index before returning record count
        self.refresh()
        n = self.count()
        logger.info('Datastore now contains {} patient records'.format(n))

    def index_patient(self, patient):
        """Index the provided models.Patient object

        If a patient with the same id already exists in the index, the patient is replaced.
        """
        id = patient.get_id()
        data = patient.to_index()

        self.save(id=id, doc=data)
        logger.info("Indexed patient: {!r}".format(id))

    def match(self, phenotypes, genes, n=10):
        """Return an elasticsearch_dsl.Response of the most similar patients to a list of phenotypes and candidate genes

        phenotypes - a list of HPO term IDs (including implied terms)
        genes - a list of ENSEMBL gene IDs for candidate genes
        """
        query_parts = []
        for id in phenotypes:
            query_parts.append(Q('match', phenotype=id))

        for gene_id in genes:
            query_parts.append(Q('match', gene=gene_id))

        query = Q('bool', should=query_parts)
        s = self.search()
        s = s.query(query)[:n]
        response = s.execute()
        return response
