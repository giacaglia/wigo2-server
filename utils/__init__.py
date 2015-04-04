from __future__ import absolute_import
from collections import namedtuple
from functools import partial, wraps

from string import punctuation
from datetime import datetime
import re
from schematics.models import Model

EPOCH = datetime(year=1970, month=1, day=1)
EMAIL_RE = re.compile(r'[^@]+@([^@]+\.[^@]+)')
Email = namedtuple('Email', ['email', 'domain', 'root', 'tld'])


def epoch(input=None):
    if input is None:
        input = datetime.utcnow()
    return (input - EPOCH).total_seconds()


def strip_punctuation(word):
    return ''.join(ch for ch in word if not is_punctuation(ch))


def is_punctuation(c):
    return c in punctuation


def strip_unicode(s):
    # re.sub(r" [^a-z0-9]+ "," ",str)`
    # t = s.decode('unicode_escape')
    response = s.encode('ascii', 'ignore')
    return response


class SecurityException(Exception):
    def __init__(self, message=None):
        super(SecurityException, self).__init__(message)
        self.code = 403


class ValidationException(Exception):
    def __init__(self, message=None, field=None, message_code='invalid'):
        super(Exception, self).__init__(message)
        self.code = 400
        self.field = field
        self.message_code = message_code


def parse_email(email):
    if isinstance(email, (str, unicode)):
        email = email.strip().lower()
        m = EMAIL_RE.match(email)
        if m:
            domain = m.group(1)
            split = domain.split('.')
            return Email(email, domain, str.join('.', split[-2:]), split[-1])
    return None


def check_email(email):
    if not isinstance(email, (str, unicode)):
        raise ValidationException('Email address empty', 'email')
    email = email.strip().lower()
    if not parse_email(email):
        raise ValidationException('Invalid email address', 'email')
    return email


def dotget(m, path, default=None):
    result = None
    if m is not None:
        result = reduce(lambda x, y: x.get(y) if isinstance(x, dict) else x, path.split('.'), m)
    if result is None:
        result = default
    return result


def dotgets(m, *args):
    return [dotget(m, path) for path in args]


def returns_clone(func):
    """
    Method decorator that will "clone" the object before applying the given
    method.  This ensures that state is mutated in a more predictable fashion,
    and promotes the use of method-chaining.
    """
    def inner(self, *args, **kwargs):
        clone = self.clone()  # Assumes object implements `clone`.
        func(clone, *args, **kwargs)
        return clone
    inner.call_local = func  # Provide a way to call without cloning.
    return inner


def memoize(field=None):
    def inner(f):
        @wraps(f)
        def decorated(*args, **kw):
            obj = args[0]
            try:
                cache = obj.__cache
            except AttributeError:
                cache = obj.__cache = {}

            if field:
                val = getattr(obj, field, None)
                key = (f, val, args[1:], frozenset(kw.items()))
            else:
                key = (f, args[1:], frozenset(kw.items()))

            try:
                res = cache[key]
            except KeyError:
                res = cache[key] = f(*args, **kw)
            return res
        return decorated
    return inner
