"""
This is a minimum working example of a Matchmaker Exchange server.
It is intended strictly as a useful reference, and should not be
used in a production setting.
"""

from __future__ import with_statement, division, unicode_literals

import logging

from flask import Flask, request, after_this_request, jsonify, render_template
from flask_negotiate import consumes, produces
from collections import defaultdict

from .models import MatchRequest, get_backend
from .schemas import validate_request, validate_response, ValidationError


API_MIME_TYPE = 'application/vnd.ga4gh.matchmaker.v1.0+json'

# Global flask application
app = Flask(__name__.split('.')[0])
# Logger
logger = logging.getLogger(__name__)

def get_outgoing_servers():
    db = get_backend()
    response = db.servers.list()
    servers = {}
    for server in response.get('rows', []):
        if server.get('direction') == 'out':
            id = server['server_id']
            label = server['server_label']
            base_url = server['base_url']
            entry = servers.setdefault(id, {})
            entry['label'] = entry.get('label') or label
            entry['base_url'] = entry.get('base_url') or base_url

    return servers

@app.route('/', methods=['GET'])
@produces('text/html')
def index():
    servers = get_outgoing_servers()
    return render_template('index.html', servers=servers)

@app.route('/match', methods=['POST'])
@consumes(API_MIME_TYPE, 'application/json')
@produces(API_MIME_TYPE)
def match():
    """Return patients similar to the query patient"""

    @after_this_request
    def add_header(response):
        response.headers['Content-Type'] = API_MIME_TYPE
        return response

    logger.info("Authorizing request")
    token = request.headers.get('X-Auth-Token')
    db = get_backend()
    server = db.servers.verify(token)
    if not server:
        response = jsonify(message='X-Auth-Token not authorized')
        response.status_code = 401
        return response

    # servers = request.args.get('servers')
    # timeout = request.args.get('timeout', 5000)

    logger.info("Getting flask request data")
    request_json = request.get_json(force=True)

    logger.info("Validate request syntax")
    try:
        validate_request(request_json)
    except ValidationError as e:
        response = jsonify(message='Request does not conform to API specification:\n{}'.format(e))
        response.status_code = 422
        return response

    logger.info("Parsing query")
    request_obj = MatchRequest.from_api(request_json)

    logger.info("Finding similar patients")
    response_obj = request_obj.match(n=5)

    logger.info("Serializing response")
    response_json = response_obj.to_api()

    logger.info("Validate response syntax")
    try:
        validate_response(response_json)
    except ValidationError as e:
        # log to console and return response anyway
        logger.error('Response does not conform to API specification:\n{}'.format(e))
        print(type(response_json['results'][0]['patient']))

    return jsonify(response_json)

