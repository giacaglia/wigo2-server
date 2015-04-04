from __future__ import absolute_import

from flask import _request_ctx_stack
from flask.sessions import SecureCookieSessionInterface


def in_request_context():
    return _request_ctx_stack.top is not None


class ApiSessionInterface(SecureCookieSessionInterface):
    def open_session(self, app, req):
        if req.path.startswith('/api/'):
            return None
        else:
            return super(ApiSessionInterface, self).open_session(app, req)
