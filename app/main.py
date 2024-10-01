from abc import abstractmethod
import asyncio


class Command:
    @abstractmethod
    def send(self, socket):
        pass


class PongCommand():
    def write(self, writer):
        writer.write(b'+PONG\r\n')


async def client_connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while not reader.at_eof():
        data = await reader.read(512)
        PongCommand().write(writer)


async def main(host: str, port: int):
    srv = await asyncio.start_server(
        client_connected, host, port, reuse_port=True)
    await srv.serve_forever()


if __name__ == "__main__":
    asyncio.run(main('127.0.0.1', 6379))
