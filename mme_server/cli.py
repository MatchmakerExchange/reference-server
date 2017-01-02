"""
This provides the command-line interface for interacting with the server.
"""
from __future__ import with_statement, division, unicode_literals

import sys
import os
import logging
import unittest

from binascii import hexlify

from .auth import get_server_manager
from .compat import urlretrieve
from .models import get_backend
from .server import app


DEFAULT_HOST = '0.0.0.0'
DEFAULT_PORT = 8000

# Matchmaker Exchange benchmark dataset of 50 patients
TEST_DATA_URL = 'https://raw.githubusercontent.com/ga4gh/mme-apis/hotfix/v1.0b/testing/benchmark_patients.json'
DEFAULT_DATA_FILENAME = 'data.json'

# Human Phenotype Ontology
HPO_URL = 'http://purl.obolibrary.org/obo/hp.obo'
DEFAULT_HPO_FILENAME = 'hp.obo'

# Gene identifier mappings, from HGNC: www.genenames.org
# Ensembl Genes 83 (GRCh38.p5)
# Columns:
#   HGNC ID
#   Approved Symbol
#   Approved Name
#   Previous Symbols
#   Synonyms
#   Entrez Gene ID (supplied by NCBI)
#   Ensembl Gene ID (supplied by Ensembl)
GENE_URL = 'http://www.genenames.org/cgi-bin/download?col=gd_hgnc_id&col=gd_app_sym&col=gd_app_name&col=gd_prev_sym&col=gd_aliases&col=md_eg_id&col=md_ensembl_id&status=Approved&status_opt=2&where=&order_by=gd_app_sym_sort&format=text&limit=&hgnc_dbtag=on&submit=submit'
DEFAULT_GENE_FILENAME = 'genes.tsv'

# TODO: incorporate disease data from:
# http://www.orphadata.org/data/xml/en_product1.xml

# Logger
logger = logging.getLogger(__name__)


def quickstart(data_filename, data_url, hpo_filename, hpo_url, gene_filename, gene_url):
    index_file('hpo', hpo_filename, hpo_url)
    index_file('genes', gene_filename, gene_url)
    # Patients must be indexed AFTER vocabularies
    index_file('patients', data_filename, data_url)


def index_file(index, filename, url):
    fetch_resource(filename, url)

    with app.app_context():
        backend = get_backend()
        index_funcs = {
            'hpo': backend.vocabularies.index_hpo,
            'genes': backend.vocabularies.index_genes,
            'patients': backend.patients.index_file,
        }
        index_funcs[index](filename=filename)


def fetch_resource(filename, url):
    if os.path.isfile(filename):
        logger.info('Found local resource: {}'.format(filename))
    else:
        logger.info('Downloading file from: {}'.format(url))
        urlretrieve(url, filename)
        logger.info('Saved file to: {}'.format(filename))


def list_servers():
    with app.app_context():
        servers = get_server_manager()
        response = servers.list()
        # print header
        fields = response['fields']
        print('\t'.join(fields))

        for server in response.get('rows', []):
            print('\t'.join([repr(server[field]) for field in fields]))


def add_server(id, direction, key=None, label=None, base_url=None):
    if not label:
        label = id

    if direction == 'out' and not base_url:
        raise Exception('--base-url must be specified for outgoing servers')

    with app.app_context():
        servers = get_server_manager()
        # Generate a random key if one was not provided
        if key is None:
            key = hexlify(os.urandom(30)).decode()
        servers.add(server_id=id, server_key=key, direction=direction, server_label=label, base_url=base_url)


def remove_server(id, direction):
    with app.app_context():
        servers = get_server_manager()
        servers.remove(server_id=id, direction=direction)


def run_tests():
    suite = unittest.TestLoader().discover('.'.join([__package__, 'tests']))
    unittest.TextTestRunner().run(suite)


def add_auth_parser(parser):
    auth_parser = parser.add_parser('auth', description="Authorization sub-commands")
    subparsers = auth_parser.add_subparsers(title='subcommands')
    subparser = subparsers.add_parser('add', description="Add server authorization")
    subparser.add_argument("id", help="A unique server identifier")
    subparser.add_argument("direction", choices=["in", "out"],
        help="Direction of server authorization, 'in': the other server can send requests, 'out': this server can send requests")
    subparser.add_argument("--key", help="The secret key used to authenticate requests to/from the server (default: randomly generate a secure key)")
    subparser.add_argument("--label", help="The display name for the server ")
    subparser.add_argument("--base-url", dest="base_url", help="The base URL for sending API requests to the other server (e.g., <base-url>/match should be a valid endpoint). Must be specified for outgoing requests")
    subparser.set_defaults(function=add_server)

    subparser = subparsers.add_parser('rm', description="Remove server authorization")
    subparser.add_argument("id", help="Server identifier")
    subparser.add_argument("direction", choices=["in", "out"],
        help="Direction of server authorization, 'in': the other server can send requests, 'out': this server can send requests")
    subparser.set_defaults(function=remove_server)

    subparser = subparsers.add_parser('list', description="List all existing server authorizations")
    subparser.set_defaults(function=list_servers)


def parse_args(args):
    from argparse import ArgumentParser

    parser = ArgumentParser()
    subparsers = parser.add_subparsers(title='subcommands')
    subparser = subparsers.add_parser('quickstart',
                                      description="Initialize datastore with example data and necessary vocabularies")
    subparser.add_argument("--data-file", default=DEFAULT_DATA_FILENAME, dest="data_filename", metavar="FILE",
                           help="Load patient data from the following JSON file (will download from --data-url if file does not exist; default: %(default)s)")
    subparser.add_argument("--data-url", default=TEST_DATA_URL, dest="data_url", metavar="URL",
                           help="Download patient data from the following url (default: %(default)s)")
    subparser.add_argument("--hpo-file", default=DEFAULT_HPO_FILENAME, dest="hpo_filename", metavar="FILE",
                           help="Load HPO from the following OBO file (will download from --hpo-url if file does not exist; default: %(default)s)")
    subparser.add_argument("--hpo-url", default=HPO_URL, dest="hpo_url", metavar="URL",
                           help="Download HPO from the following url (default: %(default)s)")
    subparser.add_argument("--gene-file", default=DEFAULT_GENE_FILENAME, dest="gene_filename", metavar="FILE",
                           help="Load gene mappings from the following TSV file (will download from --gene-url if file does not exist; default: %(default)s)")
    subparser.add_argument("--gene-url", default=GENE_URL, dest="gene_url", metavar="URL",
                           help="Download gene mappings from the following url (default: %(default)s)")
    subparser.set_defaults(function=quickstart)

    subparser = subparsers.add_parser('index', description="Index a set of patients or vocabulary")
    subparser.add_argument("index", choices=['hpo', 'genes', 'patients'])
    subparser.add_argument("--filename", metavar="FILE",
                           help="Load data from the following file (will download from --url if file does not exist)")
    subparser.add_argument("--url", dest="url", metavar="URL",
                           help="Download data from the following url")
    subparser.set_defaults(function=index_file)

    subparser = subparsers.add_parser('start', description="Start running a simple Matchmaker Exchange API server")
    subparser.add_argument("-p", "--port", default=DEFAULT_PORT,
                           dest="port", type=int, metavar="PORT",
                           help="The port the server will listen on (default: %(default)s)")
    subparser.add_argument("--host", default=DEFAULT_HOST,
                           dest="host", metavar="IP",
                           help="The host the server will listen to (0.0.0.0 to listen globally; 127.0.0.1 to listen locally; default: %(default)s)")
    subparser.set_defaults(function=app.run)

    add_auth_parser(subparsers)

    subparser = subparsers.add_parser('test', description="Run tests")
    subparser.set_defaults(function=run_tests)

    args = parser.parse_args(args)
    if not hasattr(args, 'function'):
        parser.error('a subcommand must be specified')
    return args


def main(args=sys.argv[1:]):
    logging.basicConfig(level='INFO')
    args = parse_args(args)

    # Call the function for the corresponding subparser
    kwargs = vars(args)
    function = kwargs.pop('function')
    function(**kwargs)


if __name__ == '__main__':
    sys.exit(main())
