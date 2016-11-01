# -*- coding: utf-8 -*-

from __future__ import absolute_import

import asyncio
from concurrent.futures import ThreadPoolExecutor
import contextlib
from os import path
import logging

from asynctest import TestCase
import thriftpy
# Until the client code is created, we use the sync client interface.
from thriftpy.protocol import TBinaryProtocolFactory
from thriftpy.rpc import make_client
from thriftpy.transport import TFramedTransportFactory
from thriftpy.asyncio import make_server

logging.basicConfig(level=logging.INFO)

addressbook = thriftpy.load(
    path.join(
        path.dirname(__file__),
        "addressbook.thrift"
    )
)


class Dispatcher(object):
    def __init__(self, loop):
        self.io_loop = loop
        self.registry = {}

    def add(self, person):
        """
        bool add(1: Person person);
        """
        if person.name in self.registry:
            return False
        self.registry[person.name] = person
        return True

    def get(self, name):
        """
        Person get(1: string name) throws (1: PersonNotExistsError not_exists);
        """
        if name not in self.registry:
            raise addressbook.PersonNotExistsError(
                'Person "{0}" does not exist!'.format(name))
        return self.registry[name]

    @asyncio.coroutine
    def remove(self, name):
        """
        bool remove(1: string name) throws (1: PersonNotExistsError not_exists)
        """
        # delay action for later
        yield from asyncio.sleep(0)

        if name not in self.registry:
            raise addressbook.PersonNotExistsError(
                'Person "{0}" does not exist!'.format(name))
        del self.registry[name]
        return True


class AsyncioRPCTestCase(TestCase):

    # A threaded executor for client calls.
    executor = ThreadPoolExecutor(max_workers=1)

    # A bind address for a host.
    host = '127.0.0.1'

    @asyncio.coroutine
    def create_server(self):
        self.server = yield from make_server(
            addressbook.AddressBookService,
            Dispatcher(self.loop),
            loop=self.loop,
            host=self.host,
            port=0,
        )
        self.port = self.server.sockets[0].getsockname()[1]
        return self.server

    @asyncio.coroutine
    def setUp(self):
        super().setUp()
        self.server = yield from self.create_server()

    @asyncio.coroutine
    def tearDown(self):
        self.server.close()
        yield from self.server.wait_closed()
        super().tearDown()

    def _client_interaction(self, method, *args, **kwargs):

        @contextlib.contextmanager
        def closing_client(client):
            yield
            client.close()

        def blocking_call():
            client = make_client(
                service=addressbook.AddressBookService,
                host=self.host,
                port=self.port,
                trans_factory=TFramedTransportFactory(),
                proto_factory=TBinaryProtocolFactory(),
            )
            with closing_client(client):
                return getattr(client, method)(*args, **kwargs)

        return asyncio.wait_for(
            fut=self.loop.run_in_executor(
                self.executor,
                blocking_call,
            ),
            timeout=3,
            loop=self.loop,
        )

    @asyncio.coroutine
    def test_synchronous_result(self):
        dennis = addressbook.Person(name='Dennis Ritchie')
        success = yield from self._client_interaction('add', dennis)
        assert success
        success = yield from self._client_interaction('add', dennis)
        assert not success
        person = yield from self._client_interaction('get', dennis.name)
        assert person.name == dennis.name

    @asyncio.coroutine
    def test_asynchronous_exception(self):
        exc = None
        try:
            yield from self._client_interaction(
                'remove',
                'Brian Kernighan',
            )
        except Exception as e:
            exc = e

        assert isinstance(exc, addressbook.PersonNotExistsError)

    @asyncio.coroutine
    def test_asynchronous_result(self):
        dennis = addressbook.Person(name='Dennis Ritchie')
        yield from self._client_interaction('add', dennis)
        success = yield from self._client_interaction('remove', dennis.name)
        assert success
