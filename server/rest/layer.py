from __future__ import absolute_import

import jwt
import logging
from datetime import datetime, timedelta
from flask.ext.restplus import fields
from flask import request, g
from server.rest import api, WigoResource
from server.security import user_token_required

PROVIDER_ID = '9b822724-1432-11e5-b140-5fe9000000fb'
KEY_ID = 'ae5e5c58-1524-11e5-b43d-5fe9000008e7'
RSA_KEY_PATH = 'data/layer.key'

logger = logging.getLogger('wigo.web')

try:
    with open(RSA_KEY_PATH, 'r') as rsa_priv_file:
        RSA_KEY = rsa_priv_file.read()
except Exception, e:
    logger.error('error reading rsa key {}'.format(e.message))
    RSA_KEY = None

@api.route('/vendor/layer/token')
class LayerTokenResource(WigoResource):
    @user_token_required
    @api.expect(api.model('Nonce', {
        'nonce': fields.String(description='Nonce', required=True)
    }))
    def post(self):
        user_id = g.user.id
        nonce = request.get_json().get('nonce')

        # Create identity token
        # Make sure you have PyJWT and PyCrypto libraries installed and imported
        id_token = jwt.encode(
            payload={
                'iss': PROVIDER_ID,  # String - The Provider ID found in the Layer Dashboard
                'prn': str(user_id),  # String - Provider's internal ID for the authenticating user
                'iat': datetime.now(),  # Integer - Time of Token Issuance in RFC 3339 seconds
                'exp': datetime.utcnow() + timedelta(seconds=30),
                # Integer - Arbitrary Token Expiration in RFC 3339 seconds
                'nce': nonce  # The nonce obtained via the Layer client SDK.
            },
            key=RSA_KEY,
            headers={
                'typ': 'JWT',  # String - Expresses a MIME Type of application/JWT
                'alg': 'RS256',  # String - Expresses the type of algorithm used to sign the token, must be RS256
                'cty': 'layer-eit;v=1',  # String - Express a Content Type of Layer External Identity Token, version 1
                'kid': KEY_ID  # String - Private Key associated with 'layer.pem', found in the Layer Dashboard
            },
            algorithm='RS256'
        )

        return {'token': id_token}
