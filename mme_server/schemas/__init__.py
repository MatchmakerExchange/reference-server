from __future__ import with_statement, division, unicode_literals

import json

from pkgutil import get_data

from jsonschema import validate, RefResolver, FormatChecker, ValidationError


SCHEMA_FILE = 'api.json'
REQUEST_SCHEMA = '#/definitions/request'
RESPONSE_SCHEMA = '#/definitions/response'

def load_schema():
    # Read resource from same directory of (potentially-zipped) module
    schema_data = get_data(__package__, SCHEMA_FILE).decode('utf-8')
    return json.loads(schema_data)


def validate_subschema(data, schema_selector):
    schema = load_schema()
    resolver = RefResolver.from_schema(schema)
    format_checker = FormatChecker()
    request_schema = resolver.resolve_from_url(schema_selector)
    validate(data, request_schema, resolver=resolver, format_checker=format_checker)


def validate_request(data):
    validate_subschema(data, REQUEST_SCHEMA)


def validate_response(data):
    validate_subschema(data, RESPONSE_SCHEMA)
