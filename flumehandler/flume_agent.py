import threading
import logging
import time
from queue import Queue, Empty, Full
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TFramedTransport
from thrift.protocol.TCompactProtocol import TCompactProtocol
from .thrift_protocol import Client


class FlumeAgent:

    def __init__(self, hosts, port, batch_size=50, max_size=10000, thread_size=3):
        self.hosts = hosts
        self.port = port
        self.batch_size = batch_size
        self.max_size = max_size
        self.send_queue = Queue(max_size)
        self.thread_size = thread_size
        self.clients = []
        self.client2host = dict()
        self.bad_hosts = dict()
        self.events = []
        self.last_exception_time = 0
        self.exception_interval = 1  # seconds
        self.recover_interval = 5  # seconds
        self.client_index = -1
        self.client_lock = threading.RLock()
        self.event_lock = threading.RLock()
        self.running = False

    def start(self):
        with self.client_lock:
            for host in self.hosts:
                try:
                    client = self._connect(host, self.port)
                    self.clients.append(client)
                    self.client2host[client] = host
                except Exception as e:
                    self.bad_hosts[host] = time.time()
                    logging.exception('Error when initializing clients, host is %s', host, exc_info=e)
        self.running = True
        recover_thread = threading.Thread(target=self._recover_runnable, daemon=True)
        recover_thread.start()
        for i in range(self.thread_size):
            send_thread = threading.Thread(target=self._send_runnable, daemon=True)
            send_thread.start()

    def stop(self):
        self.running = False
        while not self.send_queue.empty():
            client = self._get_client()
            if not client:
                logging.exception("There are no clients availabe to send records when stopping: %d batches of records left", self.send_queue.qsize())
                return
            try:
                events = self.send_queue.get_nowait()
                self._send(client, events)
            except Empty:
                return

    def flush(self):
        events = None
        with self.event_lock:
            if len(self.events) > 0:
                events = self.events
                self.events = []
        if events:
            try:
                self.send_queue.put(events, timeout=5)
            except Full as e:
                logging.exception("Send queue is full when flushing", exc_info=e)

    def put(self, event):
        with self.event_lock:
            self.events.append(event)
            size = len(self.events)
            if size >= self.batch_size:
                try:
                    self.send_queue.put_nowait(self.events)
                except Exception as e:
                    now = time.time()
                    if now - self.last_exception_time > self.exception_interval:
                        self.last_exception_time = now
                        logging.exception('The send queue is oversize the max size: %d', self.max_size, exc_info=e)
                self.events = []  # discarded if full

    def _send_runnable(self):
        while self.running:
            client = self._get_client()
            if not client:
                time.sleep(2)
                continue  # all clients are disconnected!
            try:
                # timeout can give chance to exit
                events = self.send_queue.get(block=True, timeout=3)
                self._send(client, events)
            except Empty:
                self.flush()

    def _send(self, client, events):
        try:
            client.appendBatch(events)
        except Exception as e:
            host = self.client2host[client]
            logging.exception('Error when sending events to flume, host is %s', host, exc_info=e)
            with self.client_lock:
                self.bad_hosts[host] = time.time()
                if client in self.clients:
                    self.clients.remove(client)
                try:
                    client.close()
                except Exception:
                    pass

    def _recover_runnable(self):
        while self.running:
            bad_hosts = self.bad_hosts.copy()
            for host, timestamp in bad_hosts.items():
                if time.time() - timestamp >= self.recover_interval:
                    try:
                        client = self._connect(host, self.port)
                        with self.client_lock:
                            self.clients.append(client)
                            self.client2host[client] = host
                            self.bad_hosts.pop(host)
                    except Exception as e:
                        with self.client_lock:
                            self.bad_hosts[host] = time.time()
                        logging.exception('Error when recovering clients, host is %s', host, exc_info=e)
            time.sleep(self.recover_interval)

    def _get_client(self) -> Client:
        with self.client_lock:
            if len(self.clients) == 0:
                return None  # no clients
            self.client_index += 1
            if self.client_index >= len(self.clients):
                self.client_index = 0
            return self.clients[self.client_index]

    def _connect(self, host, port) -> Client:
        socket = TSocket(host, port)
        transport = TFramedTransport(socket)
        protocol = TCompactProtocol(transport)
        client = Client(protocol)
        transport.open()
        return client