import logging
import sys
from context import config
import timeit

log = logging.getLogger('update_app')
logging.basicConfig(
    level=logging.getLevelName(config.log_level),
    # format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S", stream=sys.stdout)


def task_dec(method):
    def timed(*args, **kw):
        start = timeit.default_timer()
        result = method(*args, **kw)
        elapsed = timeit.default_timer() - start

        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((elapsed - start) * 1000)
        else:
            log.info('%r  %2.2f ms'.format(
                  method.__name__, (elapsed - start) * 1000))
        return result

    return timed