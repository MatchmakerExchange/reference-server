"""
This is a minimum working example of a Matchmaker Exchange server.
It is intended strictly as a useful reference, and should not be
used in a production setting.
"""

from __future__ import with_statement, division, unicode_literals

import logging
import json

from flask import Flask, request, after_this_request, jsonify
from flask_negotiate import consumes, produces
from collections import defaultdict
from werkzeug.exceptions import BadRequest

from .compat import urlopen, Request
from .decorators import auth_token_required
from .models import MatchRequest, get_backend
from .schemas import validate_request, validate_response, ValidationError


API_MIME_TYPE = 'application/vnd.ga4gh.matchmaker.v1.0+json'

# Global flask application
app = Flask(__name__.split('.')[0])
# app.config['DEBUG'] = True

# Logger
logger = logging.getLogger(__name__)

class InvalidXAuthToken(Exception):
    pass

def authenticate_request(request):
    logger.info("Authenticating request")
    token = request.headers.get('X-Auth-Token')
    db = get_backend()
    server = db.servers.verify(token)
    if not server:
        raise InvalidXAuthToken()

    return server


@app.route('/v1/match', methods=['POST'])
@consumes(API_MIME_TYPE, 'application/json')
@produces(API_MIME_TYPE)
@auth_token_required()
def match():
    """Return patients similar to the query patient"""

    @after_this_request
    def add_header(response):
        response.headers['Content-Type'] = API_MIME_TYPE
        return response

    try:
        logger.info("Getting flask request data")
        request_json = request.get_json(force=True)
    except BadRequest:
        error = jsonify(message='Invalid request JSON')
        error.status_code = 400
        return error

    try:
        logger.info("Validate request syntax")
        validate_request(request_json)
    except ValidationError as e:
        error = jsonify(message='Request does not conform to API specification',
                        request=request_json)
        error.status_code = 422
        return error

    logger.info("Parsing query")
    request_obj = MatchRequest.from_api(request_json)

    logger.info("Finding similar patients")
    response_obj = request_obj.match(n=5)

    logger.info("Serializing response")
    response_json = response_obj.to_api()

    try:
        logger.info("Validating response syntax")
        validate_response(response_json)
    except ValidationError as e:
        # log to console and return response anyway
        logger.error('Response does not conform to API specification:\n{}\n\nResponse:\n{}'.format(e, response_json))

    return jsonify(response_json)
