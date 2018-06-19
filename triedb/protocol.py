"""
Simplified Redis Serialization Protocol (RESP). Can be used for both server and client sides.
Examples:
    Simple string example: "+OK\r\n"
    Error example: "-Error message\r\n"
    Integer example: ":1134\r\n"
    Bulk string examples: "$6\r\nfoobar\r\n", "$0\r\n\r\n"
        Null: "$-1\r\n"
    Array examples:
        Empty: "*0\r\n"
        [b'foo', b'bar']: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        [1, 2, 3]: "*3\r\n:1\r\n:2\r\n:3\r\n"
        Null array: "*-1\r\n"
        [b'foo', nil, b'bar']: *3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n
"""

from typing import Union
import asyncio
from collections import namedtuple

from triedb.exceptions import (TrieDbBadRequest,
                               TrieDbProtocolError,
                               TrieDbConnectionError)

__all__ = ['process_reader',
           'process_writer']


Error = namedtuple('Error', ['message'])

CLIENT_CONNECTION_TIMEOUT = 300


async def process_reader(reader: asyncio.StreamReader):
    try:
        command = await asyncio.wait_for(reader.read(1),
                                         CLIENT_CONNECTION_TIMEOUT)
    except asyncio.TimeoutError:
        raise TrieDbConnectionError(b'Timeout error')
    if not command:
        raise TrieDbConnectionError(b'Empty request')
    try:
        return await {
            b'+': _process_simple_string,
            b'-': _process_error,
            b':': _process_integer,
            b'$': _process_string,
            b'*': _process_array
        }[command](reader)
    except KeyError:
        raise TrieDbBadRequest(b'Bad first byte')


async def _process_simple_string(reader: asyncio.StreamReader):
    return (await reader.readline()).strip()


async def _process_error(reader: asyncio.StreamReader):
    return Error((await reader.readline()).strip())


async def _process_integer(reader: asyncio.StreamReader):
    return int((await reader.readline()).strip())


async def _process_string(reader: asyncio.StreamReader):
    length = int((await reader.readline()).strip())
    if length == -1:
        return None
    return (await reader.read(length + 2))[:-2]


async def _process_array(reader: asyncio.StreamReader):
    num_elements = int((await reader.readline()).strip())
    if num_elements == -1:
        return [None]
    if num_elements < 0:
        raise TrieDbBadRequest(b'Bad array length')
    return [(await process_reader(reader)) for _ in range(num_elements)]


async def process_writer(writer: asyncio.StreamWriter,
                         data: Union[bytes,
                                     int,
                                     Error,
                                     list,
                                     tuple,
                                     None]):
    _write_response(writer, data)
    await writer.drain()


def _write_response(writer: asyncio.StreamWriter,
                    data: Union[bytes,
                                int,
                                Error,
                                list,
                                tuple,
                                None]):
    if isinstance(data, bytes):
        writer.write(b'$%d\r\n%s\r\n' % (len(data), data))
    elif isinstance(data, int):
        writer.write(b':%d\r\n' % data)
    elif isinstance(data, Error):
        writer.write(b'-%s\r\n' % data.message)
    elif isinstance(data, (list, tuple)):
        writer.write(b'*%d\r\n' % len(data))
        for item in data:
            _write_response(writer, item)
    elif data is None:
        writer.write(b'$-1\r\n')
    else:
        raise TrieDbProtocolError(b'Unrecognized type: %s' % type(data))
