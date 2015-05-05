from __future__ import absolute_import
from itertools import tee

import re
import six
import signal

from collections import namedtuple
from functools import wraps
from string import punctuation
from datetime import datetime

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


class Version(object):
    def __init__(self, version):
        super(Version, self).__init__()

        parts = version.split('.')
        cleaned = []
        for part in parts[0:3]:
            if len(part) == 0:
                cleaned.append(0)
            elif len(parts) == 2 and len(part) > 1 and part.startswith('0'):
                cleaned.append(0)
                cleaned.append(int(part))
            else:
                cleaned.append(int(part))

        while len(cleaned) < 3:
            cleaned.append(0)

        self.version = tuple(cleaned[0:3])

    def __eq__(self, other):
        return self.version == other.version

    def __hash__(self):
        return hash(self.version)

    def __cmp__(self, other):
        return cmp(self.version, other.version)


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
                cache = obj._cache
            except AttributeError:
                cache = obj._cache = {}

            key = (f, args[1:], frozenset(kw.items()))

            try:
                res = cache[key]
            except KeyError:
                res = cache[key] = f(*args, **kw)
            return res

        return decorated

    return inner


def prefix_score(v, next=False):
    if isinstance(v, six.text_type):
        v = v.encode('utf-8')
    # We only get 7 characters of score-based prefix.
    score = 0
    for ch in six.iterbytes(v[:7]):
        score *= 258
        score += ch + 1
    if next:
        score += 1
    score *= 258 ** max(0, 7 - len(v))
    return repr(bigint_to_float(score))


def partition(iterable, pred):
    t1, t2 = tee(iterable)
    return [item for item in t1 if pred(item)], [item for item in t2 if not pred(item)]


def bigint_to_float(v):
    assert isinstance(v, six.integer_types)
    sign = -1 if v < 0 else 1
    v *= sign
    assert v < 0x7fe0000000000000
    exponent, mantissa = divmod(v, 2 ** 52)
    return sign * (2 ** 52 + mantissa) * 2.0 ** (exponent - 52 - 1022)


class BreakHandler:
    """
    Trap CTRL-C, set a flag, and keep going.  This is very useful for
    gracefully exiting database loops while simulating transactions.

    To use this, make an instance and then enable it.  You can check
    whether a break was trapped using the trapped property.

    # Create and enable a break handler.
    ih = BreakHandler()
    ih.enable()
    for x in big_set:
        complex_operation_1()
        complex_operation_2()
        complex_operation_3()
        # Check whether there was a break.
        if ih.trapped:
            # Stop the loop.
            break
    ih.disable()
    # Back to usual operation...
    """

    def __init__(self, emphatic=9):
        """
        Create a new break handler.

        @param emphatic: This is the number of times that the user must
                    press break to *disable* the handler.  If you press
                    break this number of times, the handler is automagically
                    disabled, and one more break will trigger an old
                    style keyboard interrupt.  The default is nine.  This
                    is a Good Idea, since if you happen to lose your
                    connection to the handler you can *still* disable it.
        """
        self._count = 0
        self._enabled = False
        self._emphatic = emphatic
        self._oldhandler = None
        return

    def _reset(self):
        """
        Reset the trapped status and count.  You should not need to use this
        directly; instead you can disable the handler and then re-enable it.
        This is better, in case someone presses CTRL-C during this operation.
        """
        self._count = 0
        return

    def enable(self):
        """
        Enable trapping of the break.  This action also resets the
        handler count and trapped properties.
        """
        if not self._enabled:
            self._reset()
            self._enabled = True
            self._oldhandler = signal.signal(signal.SIGINT, self)
        return

    def disable(self):
        """
        Disable trapping the break.  You can check whether a break
        was trapped using the count and trapped properties.
        """
        if self._enabled:
            self._enabled = False
            signal.signal(signal.SIGINT, self._oldhandler)
            self._oldhandler = None
        return

    def __call__(self, signame, sf):
        """
        An break just occurred.  Save information about it and keep
        going.
        """
        self._count += 1
        # If we've exceeded the "emphatic" count disable this handler.
        if self._count >= self._emphatic:
            self.disable()
        return

    def __del__(self):
        """
        Python is reclaiming this object, so make sure we are disabled.
        """
        self.disable()
        return

    @property
    def count(self):
        """
        The number of breaks trapped.
        """
        return self._count

    @property
    def trapped(self):
        """
        Whether a break was trapped.
        """
        return self._count > 0