from gevent import monkey

monkey.patch_all()

if __name__ == '__main__':
    import sys
    from rq.cli import worker

    sys.argv.extend(['-w', 'utils.gevent.GeventWorker'])
    sys.argv.extend(['--results-ttl', '0'])
    worker()
