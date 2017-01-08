"""
Module hanlding the authentication of incoming and outgoing server requests.

Stores:
* Authenticated servers (`servers` index)
"""
from __future__ import with_statement, division, unicode_literals

import logging
import flask

from functools import wraps

from flask import request, jsonify

from .backend import get_backend


logger = logging.getLogger(__name__)


def auth_token_required():
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            logger.info("Authenticating request")
            token = request.headers.get('X-Auth-Token')
            backend = get_backend()
            servers = backend.get_manager('servers')
            server = servers.verify(token)
            if not server:
                error = jsonify(message='X-Auth-Token not authorized')
                error.status_code = 401
                return error

            # Set authenticated server as flask global for request
            flask.g.server = server
            return f(*args, **kwargs)
        return decorated_function
    return decorator
