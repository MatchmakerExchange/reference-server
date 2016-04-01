"""
Usage: python server.py

This is a minimum working example of a Matchmaker Exchange server.
It is intended strictly as a useful reference, and should not be
used in a production setting.
"""

from __future__ import with_statement, division, unicode_literals

import sys
import logging

from flask import Flask, request, after_this_request, jsonify
from flask_negotiate import consumes, produces

import models
from schemas import validate_request, validate_response, ValidationError


DEFAULT_HOST = '0.0.0.0'
DEFAULT_PORT = 8000
API_MIME_TYPE = 'application/vnd.ga4gh.matchmaker.v1.0+json'

# Global flask application
app = Flask(__name__)
# Logger
logger = logging.getLogger(__name__)


@app.route('/match', methods=['POST'])
@consumes(API_MIME_TYPE, 'application/json')
@produces(API_MIME_TYPE)
def match():
    """Return patients similar to the query patient"""
    @after_this_request
    def add_header(response):
        response.headers['Content-Type'] = API_MIME_TYPE
        return response

    logging.info("Getting flask request data")
    request_json = request.get_json(force=True)

    logging.info("Validate request syntax")
    try:
        validate_request(request_json)
    except ValidationError as e:
        response = jsonify(message='Request does not conform to API specification:\n{}'.format(e))
        response.status_code = 422
        return response

    logging.info("Parsing query")
    request_obj = models.MatchRequest.from_api(request_json)

    logging.info("Finding similar patients")
    response_obj = request_obj.match()

    logging.info("Serializing response")
    response_json = response_obj.to_api()

    logging.info("Validate response syntax")
    try:
        validate_response(response_json)
    except ValidationError as e:
        # log to console and return response anyway
        logging.error('Response does not conform to API specification:\n{}'.format(e))

    return jsonify(response_json)


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
