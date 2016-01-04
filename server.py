"""
Usage: python server.py

This is a minimum working example of a Matchmaker Exchange server.
It is intended strictly as a useful reference, and should not be
used in a production setting.
"""

from __future__ import with_statement, division, unicode_literals

import sys
import logging
import json

import api
from flask import Flask, request, make_response, after_this_request

DEFAULT_HOST = '0.0.0.0'
DEFAULT_PORT = 8000
API_MIME_TYPE = 'application/vnd.ga4gh.matchmaker.v1.0+json'

# Global flask application
app = Flask(__name__)
# Logger
logger = logging.getLogger(__name__)


# def add_content_type_header(response):
#     response.headers['Content-Type'] = API_MIME_TYPE

@app.route('/match', methods=['POST'])
def match():
    """Return patients similar to the query patient"""
    logging.info("Getting flask request data")
    data = request.get_json(force=True)
    logging.info("Parsing query")
    query = api.MatchRequest(data)
    logging.info("Finding similar patients")
    matches = api.match(query)
    logging.info("Serializing response")
    return (json.dumps(matches.to_json()), 200, {})


def parse_args(args):
    from argparse import ArgumentParser

    description = __doc__.strip()

    parser = ArgumentParser(description=description)
    parser.add_argument("-p", "--port", default=DEFAULT_PORT,
                        dest="port", type=int, metavar="PORT",
                        help="The port the server will listen on")
    parser.add_argument("--host", default=DEFAULT_HOST,
                        dest="host", metavar="IP",
                        help="The host the server will listen to (0.0.0.0 to listen globally)")

    return parser.parse_args(args)


def main(args=sys.argv[1:]):
    args = parse_args(args)
    logging.basicConfig(level='INFO')
    app.run(host=args.host, port=args.port)


if __name__ == '__main__':
    sys.exit(main())
