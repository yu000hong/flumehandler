
import logging
from .thrift_ttypes import ThriftFlumeEvent
from .flume_agent import FlumeAgent


class FlumeHandler(logging.Handler):

    def __init__(self, flume_agent: FlumeAgent, **kwargs):
        super().__init__()
        self.flume_agent = flume_agent
        self.meta = kwargs
        self.env = dict()

    def set_meta(self, k, v):
        self.meta[k] = v

    def set_env(self, *args, **kwargs):
        if len(args) % 2 != 0:
            raise Exception("please input key value pair")
        i = 0
        while 2*i+1 < len(args):
            self.env[args[2*i]] = args[2*i+1]
            i += 1
        self.env.update(kwargs)

    def flush(self):
        super().flush()
        self.flume_agent.flush()

    def close(self):
        super().close()
        self.flume_agent.stop()

    def emit(self, record):
        event = self.convert(record)
        self.flume_agent.put(event)

    def convert(self, record):
        headers = self.evaluate(self.meta)
        record.args.update(self.evaluate(self.env))
        body = bytes(self.format(record), 'utf8')
        return ThriftFlumeEvent(headers=headers, body=body)

    def evaluate(self, d):
        result = dict()
        for k, v in d.items():
            if callable(v):
                result[k] = v()
            else:
                result[k] = v
        return result
