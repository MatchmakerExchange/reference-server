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
app.config['DEBUG'] = True
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
            servers[id] = server

    return servers

def authenticate_request(request):
    logger.info("Authenticating request")
    token = request.headers.get('X-Auth-Token')
    db = get_backend()
    server = db.servers.verify(token)
    if not server:
        raise InvalidXAuthToken()

def send_request(server, request, timeout):
    try:
        from urllib2 import urlopen, Request
    except ImportError:
        from urllib.request import urlopen, Request

    base_url = server['base_url']
    assert base_url.startswith('https://')

    url = '{}/{}'.format(base_url, match)
    headers = [
        ('User-Agent', 'exchange-of-matchmakers/0.1'),
        ('Content-Type', API_MIME_TYPE),
        ('Accept', API_MIME_TYPE),
    ]

    auth_token = server.get('server_key')
    if auth_token:
        headers.append(('X-Auth-Token', auth_token))

    req = Request(url, data=request, headers=headers)
    handler = urlopen(req)
    response = handler.read()
    return response

def proxy_request(request, timeout=5):
    from multiprocessing import Pool

    db = get_backend()
    response = db.servers.list()

    servers = response.get('rows', [])
    is_outgoing = lambda server: server.get('direction') == 'out'
    servers = filter(is_outgoing, servers)
    if servers:
        pool = Pool(processes=3)

        handles = []
        for server in servers:
            handle = pool.apply_async(send_request, (server, request, timeout))
            handles.append((server, handle))

    results = []
    for server, handle in handles:
        try:
            result = handle.get(timeout=timeout)
        except TimeoutError:
            print('Timed out')
            result = None

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

@app.route('/match', methods=['POST'])
@consumes(API_MIME_TYPE, 'application/json')
@produces(API_MIME_TYPE)
def match():
    """Return patients similar to the query patient"""

    @after_this_request
    def add_header(response):
        response.headers['Content-Type'] = API_MIME_TYPE
        return response

    try:
        authenticate_request(request)

        timeout = request.args.get('timeout', 5000)

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


@app.route('/servers/:server/match', methods=['POST'])
@consumes(API_MIME_TYPE, 'application/json')
@produces(API_MIME_TYPE)
def match():
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
        authenticate_request(request)

        timeout = request.args.get('timeout', 5000)

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
    except InvalidXAuthToken:
        response = jsonify(message='X-Auth-Token not authorized')
        response.status_code = 401
        return response


