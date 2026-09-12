# the process to be part of the election ring

"""
Flow:
a node receives uuid from previous node, forward it if uuid > itself, otherwise ignore
if received_uuid == curr uuid -> curr node is a leader

        if received_uuid > curr_Node.uuid:
            forwande(received_uuid)
        elif received_uuid < curr_Node.uuid:
            ignore()
            
        else:
            becomeLeader(currNode)
            
Node A UUID = 20
Node B UUID = 50
Node C UUID = 30
A → B → C → A

A sends 20 to B    => uuid.A < B -> ignore
B sends 50 to C    => uuid.B < C -> C receives 50 and sends it A 
C sends 30 to A    => uuid.A < C -> A receives 30 and sends it B

C sends 50 to A  => A receives 50 and sends to B
A sends 30 to B  => B ignore
B sends 50 to C    => uuid.B < C -> C receives 50 and sends it A 

C sends 50 to A  => A receives 50 and sends to B
A sends 50 to B  => B becomes leader -> sends message to announce leader to other nodes

B ──(50, flag=1)──▶ C
C ──(50, flag=1)──▶ A
A ──(50, flag=1)──▶ B
            
"""


"""
Multithread 
1. Start accept thread
2. Main thread connects to next node
3. Wait until accept thread receives previous node
4. Both TCP connections now exist
5. Send this node's initial UUID once
6. Main thread repeatedly receives, compares and forwards
7. Stop sending after leader announcement


Socket flow:
socket() -> bind() -> listen() -> accept() -> recv/sendall()

bind(): register IP address + port for socket

Ex: server_socket.bind(("192. Hammer?", 5001))


listen() -> socket be ready to receive new connection from clients

accept() -> get one connection from waiting queue to connect and return a connected socket 

accept() process sequentially one client at a time

"""






"""
Message class

- uuid.UUID uuid: indicating the sender’s UUID. 
(e.g. 123e4567-e89b-42d3-a456-556642440000)
- int flag: representing if the leader is already elected.
(1 if leader is selected, otherwise 0)


- UUID is not JSON serializable 
    convert uuid to string before serialization 
    and convert back to UUID after serialization 
"""
import uuid 
import json
import sys
import socket
import threading
import time
import logging

class Message():
    def __init__(self,sender_uuid: uuid.UUID, flag: int =0):
        self.uuid = sender_uuid
        self.flag = flag
    # serialize: convert object Message (Python object) to json(str)
    # encode: convert str to bytes to send to socket
    def serialize(self)->str:
        message = {
            "uuid": str(self.uuid),
            "flag": self.flag
        }
        return json.dumps(message)
    # decode : convert byte streams to JSON(str)
    # deserialize: JSON(str) -> message object (python object)
    @staticmethod
    def deserialize(json_str:str)->"Message":
        message = json.loads(json_str)
        return Message (
            sender_uuid=  uuid.UUID(message["uuid"]),
            flag = message["flag"]
        )
        


"""
Main program

- assign uuid for each node
- read config
- start server thread (main thread)
- connect to next node
- wait for incoming connection
- send uuid 
- run a loop to receive/ process/ forward 

"""
BUFFER_SIZE = 1024
class Node():
    def __init__(self, uuid, config):
        self.uuid = uuid
        
        self.flag = 0
        self.leader_id = None
        self.running = True
        self.config = config
        
        self.server_ip = config["server_ip"]
        self.server_port = config["server_port"]
        
        self.client_ip = config["client_ip"]
        self.client_port = config["client_port"]
        
        self.server_sock = None
        self.previous_conn = None
        self.next_node = None
        
        # each node has its own logger
        self.logger = self._setup_logger()
        
    def _setup_logger(self):
        logger = logging.getLogger(str(self.uuid))  
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            file_handler = logging.FileHandler(f"{self.uuid}.txt", mode="w", encoding="utf-8")
            formatter = logging.Formatter("%(message)s")   # chỉ in message, không thêm timestamp/level
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger
        
    # read config.txt which looks like:  10.1.1.1,5001 , 10.1.1.2,5001
    # and convert into config : server and client ip/port number
    @staticmethod
    def read_config(filename):
        # open file 
        with open(filename,"r") as file:
            # read line by line, IP address and port are delimeted by comma
            lines = file.readlines()
        
            # first line is server ip and port
            first_line = lines[0].strip()
            server_ip , server_port = first_line.split(",")
            
            # second line is client ip and port
            
            second_line = lines[1].strip()
            client_ip , client_port = second_line.split(",")
            
            config = {
                "server_ip": server_ip,
                "server_port": int(server_port),     # convert to int because socket need port as a number
                "client_ip": client_ip,
                "client_port": int(client_port),
            }
            
        
            return config
    
    
    # ================
    # Server socket to receive message from previous node
    # ================
    
    def create_server(self):
       
        # create socket TCP
        self.server_sock= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Allow reusing the port immediately after the server stops.
   
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Bind the socket to (host, port) so the OS knows where to send packets.
        self.server_sock.bind((self.server_ip, self.server_port))
        
        
        # Start listening. The argument is the backlog: how many pending
        # connections the OS will queue while we're busy in accept().
        self.server_sock.listen(1)

        self.logger.info(f"[Node {self.uuid}] Server listening on {self.server_ip}:{self.server_port}")
        pass
    
   
    def accept_previous_node(self):
        conn ,addr = self.server_sock.accept()
        
        self.previous_conn = conn # store for future usage
        # threading.current_thread().name labels the log so you can SEE that
        # different clients are served by different threads.
        thread_name = threading.current_thread().name
        self.logger.info(f"[{thread_name}] Connected by {addr}")
        
    # ================
    # Client socket to send message to next node
    # use try/except to make sure server is ready to connect
    # ================
    def connect_next_node(self):
       # make sure successor calls listen()
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try: 
                
                # connect to next 
                sock.connect((self.client_ip, self.client_port))
                self.next_sock = sock
                self.logger.info(
                f"[Node {self.uuid}] Connected to next node "
                f"{self.client_ip}:{self.client_port}"
                )
                return
            except ConnectionRefusedError:
                sock.close()
                self.logger.info(f"[Node {self.uuid}] Next node not ready, retrying...,")
                time.sleep(1)
        
        
    # send a message to successor
    def send_msg(self,message):
        self.next_sock.sendall((message.serialize()+"\n").encode("utf-8"))
        self.logger.info(f"Sent: uuid={message.uuid}, flag={message.flag}")
        
    def receive_msg(self):
        # many json can be sent in one recv() 
        # using delimeter to separate each json
        buffer = ""
        # receive data from predecessor 
        while self.running:
            data = self.previous_conn.recv(BUFFER_SIZE)
            if not data:
                self.logger.info(f"Node {self.uuid} disconnected.")
                break
            buffer+=data.decode("utf-8")
            # get each json
            while "\n" in buffer:
                json_string, buffer = buffer.split("\n", 1)

                if not json_string.strip():
                    continue

                message = Message.deserialize(json_string)
                self.process_msg(message)
    
    # message input is a python object
    # process message based on rule:
    # - uuid recieve < self.uuid -> ignore
    # - uuid recieve > self.uuid -> forward
    # - uuid recieve == self.uuid -> become leader, set flag = 1
    def process_msg(self,message):
        
        if message.uuid < self.uuid:
            msg = "less"
            
        elif message.uuid  > self.uuid:
            msg = "greater"
        else:
            msg ="same"
        if self.flag ==0:
            state = "0"
        else:
            state = f"1, leader={self.leader_id}"
        self.logger.info(
            f"Received: uuid={message.uuid}, "
            f"flag={message.flag}, "
            f"{msg}, "
            f"{state}"
        )
        if message.flag == 1:
            self.leader_msg(message)
            return 
        
        if  message.uuid < self.uuid:
            self.logger.info(
            f"Ignored: uuid={message.uuid}, flag={message.flag}"
            )   
        elif message.uuid > self.uuid:
            self.send_msg(message)
        
        else:
            self.flag = 1
            self.leader_id = self.uuid
            self.logger.info(f"Leader is decided to {self.leader_id}.")

            leader_message = Message(
                sender_uuid=self.leader_id,
                flag=1,
            )
            self.send_msg(leader_message)
            
        
    # leader annoucenment
    # if node receive a message having flag = 1 -> leader found
    
    def leader_msg(self,message):
        self.flag = 1
        self.leader_id = message.uuid
        # complete a circle -> terminate
        if message.uuid == self.uuid:
            self.logger.info(f"leader is {self.leader_id}")
            self.running = False
            return
        # non-leader : forward message
        self.send_msg(message)
        self.logger.info(f"leader is {self.leader_id}")
        self.running = False
        
    def run(self):
        self.logger.info(f"Node ID: {self.uuid}")

        self.create_server()
        
        # run each node in one thread
        server_thread = threading.Thread(target=self.accept_previous_node, daemon=True)
            
        # another thread - used to wait from previous node
        server_thread.start()
        
        input(f"[Node {self.uuid}] Press Enter when everyone is ready ...")
        # curr thread connects to next node
        self.connect_next_node()
        # wait for predecessor connect to server of curr node
        server_thread.join()
        
        initial_message = Message(
        sender_uuid=self.uuid,
        flag=0,
        )
        self.send_msg(initial_message)

    
        self.receive_msg()
        

if __name__ == "__main__":
    config_filename = sys.argv[1]
    config = Node.read_config(config_filename)
    node_id = uuid.uuid4()

    node = Node(node_id, config)
    node.run()