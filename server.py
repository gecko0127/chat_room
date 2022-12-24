"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import threading
import socket
import shutil
from pathlib import Path
import click



# Configure logging
LOGGER = logging.getLogger(__name__)


class ChatRoomServer:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host:str, port:int):
        self.signal["shutdown"]=False
        self.tcp_server = threading.Thread(target=self.tcp_server, args=(host,port))
        self.users = {}
        # users: username -- > host, port, status: online, offline
        self.tcp_server.start()
        self.tcp_server.join()

    def tcp_server(self, host:str, port:int):
        """Example TCP socket server."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()
            sock.settimeout(1)
            while True:
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
        if message_dict["message_type"] == "register":
            self.register(message_dict)
        elif message_dict["message_type"] == "send_message":
            self.send_message(message_dict)
        elif message_dict["message_type"] == "leave":
            self.users[message_dict["username"]]["online"] = False
        elif message_dict["message_type"] == "delete":
            del self.users[message_dict["username"]]
    
    def send_message(self,message_dict:dict):
        username = message_dict["username"]
        target = message_dict["target"]
        if not self.users[target]:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((message_dict["host"], message_dict["port"]))
                m = {
                    "message_type":"user_not_found"
                }
                message = json.dumps(m)
                sock.sendall(message.encode('utf-8'))
        elif not self.users[target]["online"]:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((message_dict["host"], message_dict["port"]))
                m = {
                    "message_type":"user_offline"
                }
                message = json.dumps(m)
                sock.sendall(message.encode('utf-8'))
        else:
            target_host = self.users[target]["host"]
            target_port = self.users[target]["port"]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((target_host, target_port))
                m = {
                    "message_type":"send_message",
                    "sender":username,
                    "message":message_dict["message"]
                }
                message = json.dumps(m)
                sock.sendall(message.encode('utf-8'))

    def register(self,message_dict:dict):
        username = message_dict["username"]
        host = message_dict["host"]
        port = message_dict["port"]
        if self.users[username]:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((host, port))
                m = {
                    "message_type":"username_taken"
                }
                message = json.dumps(m)
                sock.sendall(message.encode('utf-8'))
        else:
            self.users[username]={"host":host,"port":port,"online":True}
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((host, port))
                m = {
                    "message_type":"registered",
                    "message":f"Welcome {username}"
                }
                message = json.dumps(m)
                sock.sendall(message.encode('utf-8'))




@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    ChatRoomServer(host, port)


if __name__ == "__main__":
    main()