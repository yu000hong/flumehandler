
import logging
import threading
import time
from queue import Queue, Empty, Full
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TFramedTransport
from thrift.protocol.TCompactProtocol import TCompactProtocol
from .flume.thrift_protocol import Client
from .flume.ttypes import ThriftFlumeEvent


def gen_client(host, port) -> Client:
    socket = TSocket(host, port)
    transport = TFramedTransport(socket)
    protocol = TCompactProtocol(transport)
    client = Client(protocol)
    transport.open()
    return client


class FlumeHandler(logging.Handler):

    def __init__(self, hosts, port, batch_size=50, max_size=10000, thread_size=3):
        super().__init__()
        self.hosts = hosts
        self.port = port
        self.batch_size = batch_size
        self.max_size = max_size
        self.send_queue = Queue(max_size)
        self.thread_size = thread_size
        self.clients = []
        self.client2host = dict()
        self.bad_hosts = dict()
        self.records = []
        self.last_exception_time = 0
        self.exception_interval = 1  # seconds
        self.recover_interval = 5  # seconds
        self.client_index = -1
        self.client_lock = threading.RLock()
        self.stop = True

    def start(self):
        for host in self.hosts:
            try:
                client = gen_client(host, self.port)
                self.clients.append(client)
                self.client2host[client] = host
            except Exception as e:
                self.bad_hosts[host] = time.time()
                logging.exception('Error when init clients, host is %s', host, exc_info=e)
        self.stop = False
        recover_thread = threading.Thread(target=self._recover_runnable, daemon=True)
        recover_thread.start()
        for i in range(self.thread_size):
            send_thread = threading.Thread(target=self._send_runnable, daemon=True)
            send_thread.start()

    def flush(self):
        super().flush()
        records = None
        with self.lock:
            if len(self.records) > 0:
                records = self.records
                self.records = []
        if records:
            try:
                self.send_queue.put(records, timeout=5)
            except Full as e:
                logging.exception("Send queue is full when flushing", exc_info=e)

    def close(self):
        super().close()
        self.stop = True
        while not self.send_queue.empty():
            client = self.get_client()
            if not client:
                logging.exception("There are no clients availabe to send log records when closing: %d batches of records", self.send_queue.qsize())
                return
            try:
                records = self.send_queue.get_nowait()
                self._send(client, records)
            except Empty:
                return

    def emit(self, record):
        with self.lock:
            self.records.append(record)
            size = len(self.records)
            if size >= self.batch_size:
                try:
                    self.send_queue.put_nowait(self.records)
                except Exception as e:
                    now = time.time()
                    if now - self.last_exception_time > self.exception_interval:
                        self.last_exception_time = now
                        logging.exception('The record send queue is oversize the max size: %d', self.max_size, exc_info=e)
                self.records = []  # discarded if full

    def _send_runnable(self):
        while not self.stop:
            client = self.get_client()
            if not client:
                time.sleep(2)
                continue  # all clients are disconnected!
            try:
                # timeout can give chance to exit
                records = self.send_queue.get(block=True, timeout=3)
                self._send(client, records)
            except Empty:
                self.flush()

    def _send(self, client, records):
        try:
            client.appendBatch(self.to_events(records))
        except Exception as e:
            host = self.client2host[client]
            logging.exception('Error when sending records to flume, host is %s', host, exc_info=e)
            with self.client_lock:
                self.bad_hosts[host] = time.time()
                if client in self.clients:
                    self.clients.remove(client)
                try:
                    client.close()
                except Exception:
                    pass

    def _recover_runnable(self):
        while not self.stop:
            bad_hosts = self.bad_hosts.copy()
            for host, timestamp in bad_hosts.items():
                if time.time() - timestamp >= self.recover_interval:
                    try:
                        client = gen_client(host, self.port)
                        with self.client_lock:
                            self.clients.append(client)
                            self.client2host[client] = host
                            self.bad_hosts.pop(host)
                    except Exception as e:
                        with self.client_lock:
                            self.bad_hosts[host] = time.time()
                        logging.exception('Error when recovering clients, host is %s', host, exc_info=e)
            time.sleep(self.recover_interval)

    def to_events(self, records):
        events = []
        for record in records:
            headers = self._extract_extra(record)
            body = bytes(record.getMessage(), 'utf8')
            event = ThriftFlumeEvent(headers=headers, body=body)
            events.append(event)
        return events

    def get_client(self) -> Client:
        with self.client_lock:
            if len(self.clients) == 0:
                return None  # no clients
            self.client_index += 1
            if self.client_index >= len(self.clients):
                self.client_index = 0
            return self.clients[self.client_index]

    def _extract_extra(self, record):
        attrs = {
            'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename',
            'module', 'exc_info', 'exc_text', 'stack_info', 'lineno', 'funcName', 'created',
            'msecs', 'relativeCreated', 'thread',  'threadName', 'processName', 'process',
            'is_done', 'message', 'asctime',
        }
        extra = dict()
        for k, v in record.__dict__.items():
            if k not in attrs:
                extra[k] = v
        return extra
