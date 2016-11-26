"""
This is a minimum working example of a Matchmaker Exchange server.
It is intended strictly as a useful reference, and should not be
used in a production setting.
"""

from __future__ import with_statement, division, unicode_literals

import logging
import json

from flask import Flask, request, after_this_request, jsonify, render_template
from flask_negotiate import consumes, produces
from collections import defaultdict

from .models import MatchRequest, get_backend
from .schemas import validate_request, validate_response, ValidationError


API_MIME_TYPE = 'application/vnd.ga4gh.matchmaker.v1.0+json'

# Global flask application
app = Flask(__name__.split('.')[0])
app.config['DEBUG'] = False
app.config['MME_PROXY_REQUESTS'] = True
app.config['MME_SERVE_STATIC_PAGES'] = True

# Logger
logger = logging.getLogger(__name__)

class InvalidXAuthToken(Exception):
    pass

class InvalidXAuthToken(Exception):
    pass

def get_outgoing_servers():
    db = get_backend()
    response = db.servers.list()
    servers = {}
    for server in response.get('rows', []):
        if server.get('direction') == 'out':
            server_id = server['server_id']
            servers[server_id] = server

    return servers

def authenticate():
    logger.info("Authenticating request")
    token = request.headers.get('X-Auth-Token')
    db = get_backend()
    server = db.servers.verify(token)
    if not server:
        raise InvalidXAuthToken()

def send_request(server, request_data, timeout):
    try:
        from urllib2 import urlopen, Request
    except ImportError:
        from urllib.request import urlopen, Request

    base_url = server['base_url']
    assert base_url.startswith('https://')

    url = '{}/match'.format(base_url)
    headers = {
        'User-Agent': 'exchange-of-matchmakers/0.1',
        'Content-Type': API_MIME_TYPE,
        'Accept': API_MIME_TYPE,
    }

    auth_token = server.get('server_key')
    if auth_token:
        headers['X-Auth-Token'] = auth_token

    print("Opening request to URL: " + url)
    req = Request(url, data=request_data, headers=headers)
    handler = urlopen(req)
    print("Loading response")
    response = handler.readall().decode('utf-8')
    response_json = json.loads(response)
    print("Loaded response: {!r}".format(response_json))
    return response_json

def proxy_request(request_data, timeout=5, server_ids=None):
    from multiprocessing import Pool

    db = get_backend()
    all_servers = db.servers.list().get('rows', [])

    servers = []
    for server in all_servers:
        if (server.get('direction') == 'out' and
            (not server_ids or server['server_id'] in server_ids)):
            servers.append(server)

    if servers:
        pool = Pool(processes=4)

        handles = []
        for server in servers:
            handle = pool.apply_async(send_request, (server, request_data, timeout))
            handles.append((server, handle))

    results = []
    for server, handle in handles:
        try:
            result = handle.get(timeout=timeout)
        except TimeoutError:
            print('Timed out')
            result = None
        except Exception as e:
            print('Other error: {}'.format(e))
            continue

        results.append((server, result))
    return results

@app.route('/', methods=['GET'])
@produces('text/html')
def index():
    if not app.config.get('MME_SERVE_STATIC_PAGES'):
        response = jsonify(message='Not Found')
        response.status_code = 404
        return response

    servers = get_outgoing_servers()
    return render_template('index.html', servers=servers)

@app.route('/v1/match', methods=['POST'])
@consumes(API_MIME_TYPE, 'application/json')
@produces(API_MIME_TYPE)
def match():
    """Return patients similar to the query patient"""

    @after_this_request
    def add_header(response):
        response.headers['Content-Type'] = API_MIME_TYPE
        return response

    try:
        authenticate()

        timeout = int(request.args.get('timeout', 5))

        logger.info("Getting flask request data")
        request_json = request.get_json(force=True)

        logger.info("Validate request syntax")
        try:
            validate_request(request_json)
        except ValidationError as e:
            response = jsonify(message='Request does not conform to API specification:\n{}\n\nRequest:\n{}'.format(e, request_json))
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
            logger.error('Response does not conform to API specification:\n{}\n\nResponse:\n{}'.format(e, response_json))

        return jsonify(response_json)
    except InvalidXAuthToken:
        response = jsonify(message='X-Auth-Token not authorized')
        response.status_code = 401
        return response


@app.route('/v1/servers/<server_id>/match', methods=['POST'])
@consumes(API_MIME_TYPE, 'application/json')
@produces(API_MIME_TYPE)
def match_server(server_id):
    """Proxy the match request to server :server"""
    if not app.config.get('MME_SERVE_STATIC_PAGES'):
        response = jsonify(message='Not Found')
        response.status_code = 404
        return response

    @after_this_request
    def add_header(response):
        response.headers['Content-Type'] = API_MIME_TYPE
        return response

    try:
        authenticate()

        timeout = int(request.args.get('timeout', 5))
        timeout = int(request.args.get('timeout', 5))

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

        logger.info("Preparing request")
        request_data = json.dumps(request_obj.to_api()).encode('utf-8')

        logger.info("Proxying request")
        server_responses = proxy_request(request_data, timeout=timeout, server_ids=[server_id])
        response_json = server_responses[0][1]
        logger.info("Validating response syntax")
        try:
            validate_response(response_json)
        except ValidationError as e:
            # log to console and return response anyway
            logger.error('Response does not conform to API specification:\n{}'.format(e))
            print(type(response_json['results'][0]['patient']))

        return jsonify(response_json)
    except InvalidXAuthToken:
        response = jsonify(message='X-Auth-Token not authorized')
        response.status_code = 401
        return response


