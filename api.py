import re

from flask import Flask, request, abort
from flask_restful import Resource, Api
from marshmallow import Schema, fields
from werkzeug.routing import BaseConverter

from search.index import Index

MAX_RESULTS = 10

index = Index()
print(f'Index contains {len(index)} documents')

class ExpandQuerySchema(Schema):
    fragment = fields.Str(required=True)

class CompressQuerySchema(Schema):
    address = fields.Str(required=True)

app = Flask(__name__)
api = Api(app)
schema = ExpandQuerySchema()
c_schema = CompressQuerySchema()

class Expand(Resource):
    def get(self):
        errors = schema.validate(request.args)
        if errors:
            abort(400, str(errors))
        fragment = request.args['fragment']
        fragment = fragment.replace('.',' ').replace('-',' ').replace('_',' ')
        res = index.search(fragment, search_type='AND')
        return [{'address':'0x'+r.title, 'url':r.url} for r in res[:MAX_RESULTS] ]

class Compress(Resource):
    def get(self):
        errors = c_schema.validate(request.args)
        if errors:
            abort(400, str(errors))
        found = None
        address = request.args['address']
        all_match = re.findall(pattern='0x[a-fA-F0-9]{40}$', string=address)
        if len(all_match) == 0 or len(address) != 42:
            return {'error' : 'parameter "address" is not a valid Ethereum address!'}
        address = address.replace('0x','').lower()
        for i in range(4):
            res = index.search(address[:6+i], search_type='AND')
            if len(res) == 1:
                found = i
                break
        if found:
            return {'short_address': address[:6+i], 'address': '0x'+address } 

api.add_resource(Expand, '/expand', endpoint='expand')
api.add_resource(Compress, '/compress', endpoint='compress')

# omit of you intend to use `flask run` command
if __name__ == '__main__':
    app.run(debug=True, port=5002)