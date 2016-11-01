"""The module adds support of asyncio-based handlers in thriftpy server.

Written by Dmytro Popovych.
"""
import asyncio
import functools
import logging
import struct

from thriftpy.protocol import TBinaryProtocolFactory
from thriftpy.thrift import TProcessor, TApplicationException
from thriftpy.transport import TMemoryBuffer


logger = logging.getLogger(__name__)


class AioTProcessor(TProcessor):

    @asyncio.coroutine
    def process(self, iprot, oprot):
        api, seqid, result, call = self.process_in(iprot)

        if isinstance(result, TApplicationException):
            return self.send_exception(oprot, api, result, seqid)

        try:
            call_result = call()
            if asyncio.iscoroutine(call_result):
                call_result = yield from call_result
            result.success = call_result
        except Exception as e:
            # raise if api don't have throws
            self.handle_exception(e, result)

        if not result.oneway:
            self.send_result(oprot, api, result, seqid)


class AioThriftProtocol(asyncio.Protocol):
    MAX_LENGTH = 0x3FFFFFFF

    def __init__(self, loop, processor, protocol_factory):
        self._loop = loop
        self._processor = processor
        self._protocol_factory = protocol_factory
        self._transport = None
        self._received = b''

    def connection_made(self, transport):
        self._transport = transport

    def data_received(self, data):
        self._received += data
        while len(self._received) >= 4:
            length = struct.unpack('!i', self._received[:4])[0]

            if length > self.MAX_LENGTH:
                logger.error(
                    'Frame size %d too large for AsyncThriftProtocol',
                    length,
                )
                self._transport.close()
                return

            elif length == 0:
                logger.error('Empty frame')
                self._transport.close()
                return

            if len(self._received) < length + 4:
                return

            input_frame = self._received[4:4 + length]
            self._received = self._received[4 + length:]
            self._loop.create_task(self.message_received(input_frame))

    @asyncio.coroutine
    def message_received(self, frame):
        output_buffer = TMemoryBuffer()

        yield from self._processor.process(
            iprot=self._protocol_factory.get_protocol(TMemoryBuffer(frame)),
            oprot=self._protocol_factory.get_protocol(output_buffer),
        )

        output_frame = output_buffer.getvalue()
        output_length = struct.pack('!i', len(output_frame))

        self._transport.write(output_length + output_frame)


def make_server(service, handler, host, port, loop=None):
    loop = loop or asyncio.get_event_loop()

    return loop.create_server(
        protocol_factory=functools.partial(
            AioThriftProtocol,
            loop=loop,
            processor=AioTProcessor(service, handler),
            protocol_factory=TBinaryProtocolFactory(),
        ),
        host=host,
        port=port,
    )
