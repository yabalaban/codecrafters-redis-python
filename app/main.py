import argparse
import asyncio
from dataclasses import dataclass
from enum import Enum
import os
import random
import socket
import string
import time


def _write_chunks(chunks: list[bytearray], prefix, suffix=b'') -> bytearray:
    data = bytearray()
    data.extend(prefix)
    data.extend(b'\r\n'.join(chunks))
    data.extend(suffix)
    return data


def _parse_chunk(data: bytearray, offset: int) -> tuple[bytearray, int]:
    value = bytearray()
    while not (data[offset] == ord('\r') and data[offset + 1] == ord('\n')):
        value.append(data[offset])
        offset += 1
    # skip '\r\n'
    offset += 2
    return value, offset


class RespString:
    def __init__(self, value: str):
        self.value = value

    def encode(self) -> bytearray:
        return _write_chunks([self.value.encode()], b'+', b'\r\n')

    @staticmethod
    def decode(data: bytearray, offset: int) -> tuple[any, int]:
        # skip '+' 
        offset += 1 
        value, offset = _parse_chunk(data, offset)
        return RespString(value.decode()), offset

    def __repr__(self):
        return f"RespString({self.value=})"


class RespInteger:
    def __init__(self, value: int):
        self.value = value

    def encode(self) -> bytearray:
        return _write_chunks([str(self.value).encode()], b':', b'\r\n')

    @staticmethod
    def decode(data: bytearray, offset: int) -> tuple[any, int]:
        # skip ':' 
        offset += 1 
        value, offset = _parse_chunk(data, offset)
        return RespString(int(value.decode())), offset

    def __repr__(self):
        return f"RespInteger({self.value=})"
    

class RespBulkString:
    def __init__(self, value: str | None):
        self.value = value

    def encode(self) -> bytearray:
        if self.value:
            data = self.value.encode()
            return _write_chunks([str(len(data)).encode(), data], b'$', b'\r\n')
        else:
            return b'$-1\r\n'

    @staticmethod
    def decode(data: bytearray, offset: int) -> tuple[any, int]:
        # skip '$' 
        offset += 1 
        # len – can be skipped?
        _, offset = _parse_chunk(data, offset)
        value, offset = _parse_chunk(data, offset)
        return RespBulkString(value.decode()), offset

    def __repr__(self):
        return f"RespBulkString({self.value=})"


class RespArray:
    def __init__(self, items: list[any]):
        self.items = items 

    def encode(self) -> bytearray:
        data = b''.join([item.encode() for item in self.items])
        blen = str(len(self.items)).encode()
        return _write_chunks([blen, data], b'*')

    @staticmethod
    def decode(data: bytearray, offset: int) -> tuple[any, int]:
        # skip '*' 
        offset += 1 
        # len – can be skipped?
        bl, offset = _parse_chunk(data, offset)
        l = int(bl.decode())
        items = [] 
        for _ in range(l):
            item, offset = decode(data, offset)
            items.append(item)
        return RespArray(items), offset

    def __repr__(self):
        return f"RespArray({self.items=})"


resp = {
    ord('+'): RespString,
    ord('$'): RespBulkString,
    ord('*'): RespArray,
    ord(':'): RespInteger,
}


def decode(data: bytearray, offset: int) -> tuple[any, int]: 
    return resp[data[offset]].decode(data, offset)


def ping_cmd(_, writer):
    writer.write(RespBulkString('PONG').encode())


def echo_cmd(dt, writer):
    writer.write(dt.items[1].encode())


db: dict[str, tuple[str, int]] = {}

def set_cmd(dt: RespArray, writer: asyncio.StreamWriter):
    expiration = None if len(dt.items) == 3 else round(time.time() * 1000) + int(dt.items[4].value)
    STATE.db.items[dt.items[1].value] = (dt.items[2].value, expiration)
    writer.write(RespString('OK').encode())


def get_cmd(dt: RespArray, writer: asyncio.StreamWriter):
    now = round(time.time() * 1000)
    k = dt.items[1].value 
    v = STATE.db.items.get(k)
    if not v or (v[1] and v[1] < now):
        writer.write(RespBulkString(None).encode()) 
    else:
        writer.write(RespBulkString(v[0]).encode())


def config_cmd(dt: RespArray, writer: asyncio.StreamWriter):
    def get(dt: RespArray, writer: asyncio):
        value = STATE.dir if dt.items[2].value == 'dir' else STATE.dbfilename
        writer.write(RespArray([dt.items[2], RespBulkString(value)]).encode()) 
    

    subcmds = {
        'GET': get 
    }
    subcmds[dt.items[1].value.upper()](dt, writer)


def keys_cmd(dt: RespArray, writer: asyncio.StreamWriter):
    # by default "*"
    keys = STATE.db.items.keys() 
    writer.write(RespArray([RespBulkString(x) for x in keys]).encode())


def info_cmd(dt: RespArray, writer: asyncio.StreamWriter):
    def replication(dt: RespArray, writer: asyncio):
        writer.write(STATE.replication.encode()) 

    subcmds = {
        'REPLICATION': replication
    }
    subcmds[dt.items[1].value.upper()](dt, writer)
        

cmds = {
    'PING': ping_cmd,
    'ECHO': echo_cmd,
    'SET': set_cmd,
    'GET': get_cmd,
    'CONFIG': config_cmd,
    'KEYS': keys_cmd,
    'INFO': info_cmd,
}

async def client_connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while not reader.at_eof():
        data = await reader.read(512)
        dt, _ = decode(data, 0)
        if type(dt) is RespBulkString:
            cmds[dt.value.upper()](dt, writer)
        elif type(dt) is RespArray:
            cmds[dt.items[0].value.upper()](dt, writer)


def _assert_section(data: bytearray, offset: int, value: int) -> int:
    assert data[offset] == value
    return offset + 1


def _decode_size(data: bytearray, offset: int) -> tuple[int, int, bool]: 
    flag = data[offset] >> 6 
    if flag == 0x00:
        return int(data[offset] & 0b00111111), offset + 1, False
    elif flag == 0x01:
        val = (data[offset] & 0b00111111) << 8
        val += data[offset + 1]
        return val, offset + 2
    elif flag == 0x02:
        return int.from_bytes(data[offset + 1:offset + 5], 'big'), offset + 5, False 
    elif flag == 0x03:
        return data[offset], offset + 1, True 
    else:
        assert False, 'invalid size data'


def _decode_string(data: bytearray, offset: int) -> tuple[str, int]: 
    (l, offset, ext) = _decode_size(data, offset) 
    if ext: 
        if l == 0xC0:
            l = 1 
        elif l == 0xC1:
            l = 2
        elif l == 0xC2:
            l = 4
        elif l == 0xC3:
            assert False, 'lzf compression is not implemented'
        s = str(int.from_bytes(data[offset: offset + l], 'little'))     
    else:
        s = data[offset: offset + l].decode() 
    return s, offset + l


def _decode_timestamp(data: bytearray, offset: int) -> tuple[int, int]: 
    if data[offset] == 0xFC:
        return int.from_bytes(data[offset + 1: offset + 9], 'little'), offset + 9
    elif data[offset] == 0xFD:
        return int.from_bytes(data[offset + 1: offset + 5], 'little') * 1000, offset + 5
    else:
        return 0, offset


def _parse_fragment_until(data: bytearray, offset: int, value: int) -> tuple[bytearray, int]:
    fragment = bytearray()
    while data[offset] != value:
        fragment.append(data[offset])
        offset += 1
    return fragment, offset + 1


class RedisReplicationRole(Enum):
    MASTER = "master" 
    SLAVE = "slave"


@dataclass
class RedisReplication: 
    role: RedisReplicationRole
    master_replid: str  | None   
    master_repl_offset: int
    host: tuple[str, int] | None 

    def encode(self) -> bytearray:
        payload = f"role:{self.role.value}"
        if self.role == RedisReplicationRole.MASTER:
            payload += f'\r\nmaster_replid:{self.master_replid}'
            payload += f'\r\nmaster_repl_offset:{self.master_repl_offset}'
        return RespBulkString(payload).encode()

    @staticmethod
    def master():
        return RedisReplication(
            role=RedisReplicationRole.MASTER,
            master_replid=''.join(random.choices(string.ascii_letters + string.digits, k=40)),
            master_repl_offset=0,
            host=None 
        )
    
    @staticmethod
    def slave(host: tuple[str, int]):
        return RedisReplication(
            role=RedisReplicationRole.SLAVE,
            master_replid=None,
            master_repl_offset=0,
            host=host 
        )


class RedisDatabase:
    dbfilename: str 
    rdbv: int 
    items: dict[str, tuple[str | int, int]]

    def __init__(self, 
                 dbfilename: str,
                 rdbv: int,
                 items: dict[str, tuple[str | int, int]]):
        self.rdbv = rdbv
        self.dbfilename = dbfilename
        self.items = items

    @staticmethod
    def from_bytes(dbfilename: str, data: bytearray):
        assert data[:5] == b'REDIS'
        offset = 5 
        (rdbvb, offset) = _parse_fragment_until(data, offset, 0xFA)
        # attributes
        (_, offset) = _parse_fragment_until(data, offset, 0xFE)
        # db idx
        (_, offset, _) = _decode_size(data, offset)
        offset = _assert_section(data, offset, 0xFB)
        (sizekv, offset, _) = _decode_size(data, offset)
        # size of kv with expiration
        (_, offset, _) = _decode_size(data, offset)
        items = {} 
        for _ in range(sizekv):
            (ts, offset) = _decode_timestamp(data, offset)
            assert data[offset] == 0x00 # string 
            offset += 1
            (key, offset) = _decode_string(data, offset)
            (value, offset) = _decode_string(data, offset)
            items[key] = (value, ts)
        assert data[offset] == 0xFF
        return RedisDatabase(dbfilename, rdbvb.decode(), items)


@dataclass
class InstanceState: 
    dir: str | None = None
    db: RedisDatabase | None = None
    replication: RedisReplication | None = None 


STATE = InstanceState()


def replicate():
    assert STATE.replication.role == RedisReplicationRole.SLAVE

    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket.connect(STATE.replication.host)
    clientsocket.send(RespArray([RespBulkString('PING')]).encode())


async def main(host: str):
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir')
    parser.add_argument('--dbfilename')
    parser.add_argument('--replicaof')
    parser.add_argument('--port', default=6379)
    args = parser.parse_args()
    STATE.dir = args.dir
    if args.dir and args.dbfilename and os.path.isfile(f'{args.dir}/{args.dbfilename}'):
        with open(f'{args.dir}/{args.dbfilename}', 'rb') as f:
            data = f.read() 
            STATE.db = RedisDatabase.from_bytes(args.dbfilename, data)
    else:
        STATE.db = RedisDatabase('default', 0, {})
    if args.replicaof:
        addr, rport = args.replicaof.split(' ')
        STATE.replication = RedisReplication.slave((addr, int(rport)))
        replicate()
    else:
        STATE.replication = RedisReplication.master()
    srv = await asyncio.start_server(
        client_connected, host, args.port, reuse_port=True)
    await srv.serve_forever()


if __name__ == "__main__":
    asyncio.run(main('127.0.0.1'))
