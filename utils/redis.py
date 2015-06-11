from __future__ import absolute_import

import functools
from aplus import Promise


class PPromise(Promise):
    def __init__(self, pipeline):
        Promise.__init__(self)
        self.pipeline = pipeline

    def get(self, timeout=None):
        if self.isPending:
            self.pipeline.execute()
        return Promise.get(self, timeout)


class PromisePipeline(object):
    def __init__(self, redis):
        super(PromisePipeline, self).__init__()
        self.pipeline = redis.pipeline()
        self.__pending = []

    def __wrap(self, method, *args, **kwargs):
        f = getattr(self.pipeline, method)
        f(*args, **kwargs)
        p = PPromise(self)
        self.__pending.append(p)
        return p

    def execute(self):
        results = self.pipeline.execute()
        for index, result in enumerate(results):
            self.__pending[index].fulfill(result)
        self.__pending = []
        return results

    def __getattr__(self, method):
        return functools.partial(self.__wrap, method)

