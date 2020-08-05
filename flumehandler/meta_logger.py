import logging


class MetaLogger(logging.Logger):

    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)
        self.meta = dict()

    def setMeta(self, key, value):
        self.meta[key] = value

    def fillMeta(self, **kwargs):
        if 'extra' in kwargs:
            kwargs['extra'].update(self.meta)
        else:
            kwargs['extra'] = self.meta
        return kwargs

    def debug(self, msg, *args, **kwargs):
        kwargs = self.fillMeta(**kwargs)
        super().debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        print(msg)
        kwargs = self.fillMeta(**kwargs)
        super().info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        kwargs = self.fillMeta(**kwargs)
        super().warning(msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        kwargs = self.fillMeta(**kwargs)
        super().warn(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        kwargs = self.fillMeta(**kwargs)
        super().error(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        kwargs = self.fillMeta(**kwargs)
        super().exception(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        kwargs = self.fillMeta(**kwargs)
        super().critical(msg, *args, **kwargs)

    fatal = critical

    def log(self, level, msg, *args, **kwargs):
        kwargs = self.fillMeta(**kwargs)
        super().log(level, msg, *args, **kwargs)
