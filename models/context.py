import gzip
import logging
import pickle

from utils.file import getpath


class BaseContext:
    _log = logging.getLogger(__name__)

    def save(self, file_path):
        self._log.info('Saving {} to {}'.format(self.__class__.__name__, file_path))
        ctx_file = getpath(file_path)
        with gzip.GzipFile(ctx_file, "wb") as f:
            pickle.dump(self, f, pickle.HIGHEST_PROTOCOL)

    @classmethod
    def load(cls, file_path):
        ctx_file = getpath(file_path)
        cls._log.info('Loading {} from {}'.format(cls.__name__, ctx_file))
        with gzip.GzipFile(ctx_file, "rb") as f:
            ctx = pickle.load(f)
            if isinstance(ctx, cls):
                return ctx
            else:
                raise Exception('{} is not of type {}'.format(file_path, cls.__name__))
