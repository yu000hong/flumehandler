
import logging
from .meta_logger import MetaLogger


def wrap_logger():
    logging.setLoggerClass(MetaLogger)
    tmpGetLogger = logging.getLogger

    def getLogger(name=None, **kwargs):
        logger = tmpGetLogger(name=name)
        if len(kwargs) == 0:
            return logger  # do nothing
        if isinstance(logger, MetaLogger):
            for k, v in kwargs.items():
                logger.setMeta(k, str(v))
            return logger
        if not hasattr(logger, '_meta'):
            logger._meta = {}
        for k, v in kwargs.items():
            logger._meta[k] = str(v)
        if hasattr(logger, '_wrapped'):
            return logger

        debugFunc = logger.debug
        infoFunc = logger.info
        warningFunc = logger.warning
        errorFunc = logger.error
        criticalFunc = logger.critical
        logFunc = logger.log

        def fill(**kwargs):
            if not hasattr(logger, '_meta'):
                return
            if 'extra' in kwargs:
                kwargs['extra'].update(logger._meta)
            else:
                kwargs['extra'] = logger._meta
            return kwargs

        def debug(msg, *args, **kwargs):
            kwargs = fill(**kwargs)
            debugFunc(msg, *args, **kwargs)

        def info(msg, *args, **kwargs):
            kwargs = fill(**kwargs)
            infoFunc(msg, *args, **kwargs)

        def warning(msg, *args, **kwargs):
            kwargs = fill(**kwargs)
            warningFunc(msg, *args, **kwargs)

        def error(msg, *args, **kwargs):
            kwargs = fill(**kwargs)
            errorFunc(msg, *args, **kwargs)

        def critical(msg, *args, **kwargs):
            kwargs = fill(**kwargs)
            criticalFunc(msg, *args, **kwargs)

        def log(level, msg, *args, **kwargs):
            kwargs = fill(**kwargs)
            logFunc(level, msg, *args, **kwargs)

        logger.debug = debug
        logger.info = info
        logger.warning = warning
        logger.warn = warning
        logger.error = error
        logger.exception = error
        logger.critical = critical
        logger.fatal = critical
        logger.log = log
        logger._wrapped = True
        return logger

    logging.getLogger = getLogger
