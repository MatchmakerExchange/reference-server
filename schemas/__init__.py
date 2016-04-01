import os
import json

from jsonschema import validate, RefResolver, FormatChecker, ValidationError


SCHEMA_FOLDER = 'schemas'
SCHEMA_FILE = 'api.json'
REQUEST_SCHEMA = '#/definitions/request'
RESPONSE_SCHEMA = '#/definitions/response'


def load_schema():
    with open(os.path.join(SCHEMA_FOLDER, SCHEMA_FILE)) as ifp:
        return json.load(ifp)


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

