"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import socket
import threading
import click

LOGGER = logging.getLogger(__name__)

class User:

    def __init__(self,host:str,port:int, chat_room_host:str, chat_room_port:int):
        self.host = host
        self.port = port
        self.chat_room_host = chat_room_host
        self.chat_room_port = chat_room_port
        self.signals={"shutdown":False}
        self.tcp_server.start()
        self.tcp_socket.join()
        self.username = None

    def send_register(self):
        username = input("Enter your user name: ")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.chat_room_host, self.chat_room_port))
            m = {
                "message_type":"register",
                "username": username,
                "host": self.host,
                "port": self.port
            }
            message = json.dumps(m)
            sock.sendall(message.encode('utf-8'))
            self.username = username


    def tcp_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)
            self.send_register()
            while not self.signal["shutdown"]:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])
                clientsocket.settimeout(1)
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                    interpret_message(message_dict)
                except json.JSONDecodeError:
                    continue
                time.sleep(0.1)

    def interpret_message(self,message_dict:dict):
        if message_dict["message_type"] == "username_taken":
            print(f"Username {self.username} is already taken. Please choose another name.")
            self.register()
        elif message_dict["message_type"] == "registered":
            friend_username = input("Enter your friend's username to start chatting: ")
            self.send_message(friend_username)
        elif message_dict["message_type"] == "send_message":
            message = message_dict["message"]
            sender = message_dict["sender"]
            print(f"{sender} : {message}")
            self.send_message(sender)

    def send_message(self,receiver:str):
        message = input("Me: ")
        if message = "shutdown":
            self.signal["shutdown"] = True
            return
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.chat_room_host, self.chat_room_port))
            m = {
                "message_type":"send_message",
                "username": self.username,
                "host": self.host,
                "port": self.port,
                "target":receiver
            }
            message = json.dumps(m)
            sock.sendall(message.encode('utf-8'))




@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--chat-room-host", "chat_room_host", default="localhost")
@click.option("--chat-room-port", "chat_room_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, chat_room_host, chat_room_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"User:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    User(host, port, chat_room_host, chat_room_port)


if __name__ == "__main__":
    main()
