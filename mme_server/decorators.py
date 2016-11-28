from functools import wraps
from flask import request, jsonify

from .models import get_backend

import logging

logger = logging.getLogger(__name__)


def auth_token_required():
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            logger.info("Authenticating request")
            token = request.headers.get('X-Auth-Token')
            db = get_backend()
            server = db.servers.verify(token)
            if not server:
                error = jsonify(message='X-Auth-Token not authorized')
                error.status_code = 401
                return error

            return f(*args, **kwargs)
        return decorated_function
    return decorator
