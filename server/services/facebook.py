"""
Utilities to make dealing with the facebook api a bit easier.

Capturing the facebook error sub codes here since the facebook page seems
to disappear periodically.

From: https://developers.facebook.com/docs/howtos/login/invalid-tokens-revoked-permissions/

`error_subcode`	Meaning
456	 The session is malformed.
457	 The session has an invalid origin.
458	 The session is invalid, because the app is no longer installed.
459	 The user has been checkpointed. The error_data will contain the URL the user needs to go to to clear the checkpoint.
460	 The session is invalid likely because the user changed the password.
461	 The session is invalid, because the user has reinstalled the app.
462	 The session has a stale version.
463	 The session has expired.
464	 The session user is not confirmed.
465	 The session user is invalid.
466	 The session was explicitly invalidated through an API call.
467	 The session is invalid, because the user logged out.
468	 The session is invalid, because the user has not used the app for a long time.


"""
from __future__ import absolute_import

import logging
import socket
from urlparse import urljoin, parse_qs
from datetime import datetime
from oauthlib.oauth2 import TokenExpiredError
from requests_oauthlib import OAuth2Session
from requests_oauthlib.compliance_fixes import facebook_compliance_fix
from config import Configuration

logger = logging.getLogger('wigo.facebook')

FB_STATE_TOKEN = '34s90ifjksdd'


class Facebook(object):
    def __init__(self, token, token_expires_in):
        self.set_token(token, token_expires_in)

    def set_token(self, token, token_expires_in):
        if isinstance(token_expires_in, datetime):
            token_expires_in = int((token_expires_in - datetime.utcnow()).total_seconds())

        self.token = token

        self.session = OAuth2Session(Configuration.FACEBOOK_APP_ID, token={
            'access_token': token,
            'token_type': 'Bearer',
            'expires_in': token_expires_in
        })

        self.session = facebook_compliance_fix(self.session)

    def exchange_token(self):
        try:
            resp = self.session.get('https://graph.facebook.com/oauth/access_token', params={
                'grant_type': 'fb_exchange_token',
                'client_id': Configuration.FACEBOOK_APP_ID,
                'client_secret': Configuration.FACEBOOK_APP_SECRET,
                'fb_exchange_token': self.token
            })

            if resp.status_code == 200:
                text = resp.text
                parsed = parse_qs(text)
                if 'access_token' in parsed:
                    token = parsed.get('access_token')[0]
                    token_expires_in = int(parsed.get('expires')[0])
                    self.set_token(token, token_expires_in)
                    return token, token_expires_in
            else:
                logger.warn('error exchanging facebook token')
        except Exception, e:
            logger.exception('exception exchanging facebook token')

        return None, None

    def get(self, path):
        """ Fetches the data from facebook, and returns the nested 'data' attribute from it. """

        try:
            results = self.session.get(urljoin('https://graph.facebook.com', path))
            if results.status_code == 200:
                data = results.json()
                if 'data' in data:
                    data = data.get('data')
                return data
            else:
                self.raise_fb_error(results)
        except Exception, e:
            self.handle_exception(e)

    def iter(self, path, timeout=10):
        """ Iterator over facebook list results. """

        try:
            while path:
                results = self.session.get(urljoin('https://graph.facebook.com', path))
                if results.status_code == 200:
                    data = results.json()
                    if 'data' in data:
                        data = data.get('data')
                    if data:
                        for item in data:
                            yield item
                    if 'paging' in data and 'next' in data.get('paging'):
                        next_url = data.get('paging').get('next')
                        path = next_url[next_url.find('facebook.com/') + len('facebook.com/'):]
                    else:
                        path = None
                else:
                    self.raise_fb_error(results)
        except Exception, e:
            self.handle_exception(e)

    def raise_fb_error(self, results):
        if isinstance(results.json(), dict):
            error = results.json().get('error')
            code = error.get('code')
            subcode = error.get('error_subcode')

            if code == 190:
                raise FacebookTokenExpiredException(refreshable=subcode in (460, 461, 462, 463))
            else:
                raise FacebookException(message=error.get('message'),
                                        type=error.get('type'), code=code, subcode=subcode)
        else:
            raise FacebookException(message=results.data)

    def handle_exception(self, e):
        if isinstance(e, TokenExpiredError):
            print e.json
            raise FacebookTokenExpiredException()
        elif isinstance(e, socket.timeout):
            raise FacebookTimeoutException('Timeout')
        else:
            raise e


class FacebookException(Exception):
    def __init__(self, message=None, type=None, code=None, subcode=None):
        super(FacebookException, self).__init__(message)
        self.type = type
        self.code = code
        self.subcode = subcode


class FacebookTokenExpiredException(FacebookException):
    def __init__(self, refreshable=True):
        super(FacebookTokenExpiredException, self).__init__(message='Access token expired', code='expired_token')
        self.refreshable = refreshable


class FacebookTimeoutException(FacebookException):
    pass
