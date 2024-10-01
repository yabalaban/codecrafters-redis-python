import asyncio


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
    

class RespBulkString:
    def __init__(self, value: str):
        self.value = value

    def encode(self) -> bytearray:
        data = self.value.encode()
        return _write_chunks([str(len(data)).encode(), data], b'$', b'\r\n')

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
}


def decode(data: bytearray, offset: int) -> (any, int): 
    return resp[data[offset]].decode(data, offset)


def ping_cmd(_, writer):
    writer.write(RespBulkString('PONG').encode())


def echo_cmd(dt, writer):
    writer.write(dt.items[1].encode())


cmds = {
    'PING': ping_cmd,
    'ECHO': echo_cmd,
}

async def client_connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while not reader.at_eof():
        data = await reader.read(512)
        dt, _ = decode(data, 0)
        if type(dt) is RespBulkString:
            cmds[dt.value](dt, writer)
        elif type(dt) is RespArray:
            cmds[dt.items[0].value](dt, writer)


async def main(host: str, port: int):
    srv = await asyncio.start_server(
        client_connected, host, port, reuse_port=True)
    await srv.serve_forever()


if __name__ == "__main__":
    asyncio.run(main('127.0.0.1', 6379))
