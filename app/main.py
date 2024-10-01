import socket  # noqa: F401


class Command:
    def send(self, socket):
        pass


class PongCommand(Command):
    def send(self, socket):
        socket.send(b'+PONG\r\n')


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    (sock, addr) = server_socket.accept() # wait for client
    with sock:
        pong = PongCommand()
        pong.send(sock)
    

if __name__ == "__main__":
    main()
