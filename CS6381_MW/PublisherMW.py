import zmq  # ZMQ sockets
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from CS6381_MW.Common import PinguMW
from functools import wraps
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from kazoo.recipe.election import Election
from kazoo.recipe.watchers import DataWatch
import json
import timeit
import time

class PublisherMW(PinguMW):
  def handle_exception(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
      try:
        return func(*args, **kwargs)
      except Exception as e:
        raise e
    return wrapper
  
  def __init__(self, logger):
    super().__init__(logger)
    self.req = None # will be a ZMQ REQ socket to talk to Discovery service
    self.pub = None # will be a ZMQ PUB socket for dissemination
    self.zk = None 
    self.disc = None 
    self.name = None 

  @handle_exception
  def configure(self, args):
    self.logger.info("PublisherMW::configure")
    self.port = args.port
    self.addr = args.addr
    context = zmq.Context()  # returns a singleton object
    self.poller = zmq.Poller()
    self.zk = KazooClient(hosts=args.zookeeper)
    self.req = context.socket(zmq.REQ)
    self.pub = context.socket(zmq.PUB)
    self.poller.register(self.req, zmq.POLLIN)
    self.setRequest()
    connect_str = "tcp://" + args.discovery
    self.req.connect(connect_str)
    bind_string = "tcp://*:" + str(self.port)
    self.pub.bind (bind_string)
    
    @self.zk.DataWatch("/leader")
    def watchLeader(data, stat):
      meta = json.loads(self.zk.get("/leader")[0].decode('utf-8'))
      self.logger.info("PublisherMW::watch_leader: disconnecting req and redirecting to new leader")
      if self.disc != None:
        self.req.disconnect(self.disc)
      self.req.connect(meta["repAddress"])
      self.disc = meta["repAddress"]
      self.logger.info("Successfully connected to the new leader")  
    self.logger.info("PublisherMW::configure completed")

  def event_loop(self, timeout=None):
   super().event_loop("PublisherMW", self.req, timeout)
            
  @handle_exception
  def handle_reply(self):
    self.logger.info("PublisherMW::handle_reply")
    bytesRcvd = self.req.recv()
    discovery_response = discovery_pb2.DiscoveryResp()
    discovery_response.ParseFromString(bytesRcvd)
    if discovery_response.msg_type == discovery_pb2.TYPE_REGISTER:
      timeout = self.upcall_obj.register_response(discovery_response.register_resp)
    elif discovery_response.msg_type == discovery_pb2.TYPE_ISREADY:
      timeout = self.upcall_obj.isready_response(discovery_response.isready_resp)
    else:
      raise ValueError("Unrecognized response message")
    return timeout
            

  def is_ready(self):
    super().is_ready("PublisherMW")
    
  @handle_exception
  def disseminate (self, id, topic, data, current_time):
    send_str = topic + ":" + id + ":" + data + ":" + current_time
    self.logger.info("PublisherMW::disseminate - {}".format (send_str))
    self.pub.send(bytes(send_str, "utf-8"))
            
  # here we save a pointer (handle) to the application object
  def set_upcall_handle(self, upcall_obj):
    super().set_upcall_handle(upcall_obj)
        
  def disable_event_loop(self):
    super().disable_event_loop()
    
  # New code for PA3
  """
  The register() method registers a publisher with Zookeeper by creating an ephemeral node at 
  /publisher/{name} path in Zookeeper, where name is the name of the publisher. 
  The method takes two arguments: name and topiclist. 
  The name is the name of the publisher, and topiclist is the list of topics the publisher
  is interested in. The register() method creates a dictionary with two keys: id and topiclist, 
  where id is a dictionary with id, addr and port keys. The id key contains the name of the 
  publisher, addr contains the IP address, and port contains the port number of the publisher. 
  The topiclist key contains the list of topics the publisher is interested in. 
  The dictionary is then converted to a JSON string and stored as the value of the ephemeral node. 
  Finally, the method logs a message indicating that the register message has been sent.

  The setRequest() method connects the publisher to the leader broker. It first starts the 
  Zookeeper client and waits until a leader is elected. Once a leader is elected, the method 
  gets the metadata of the leader from the /leader node in Zookeeper and connects to the 
  leader's repAddress using a ZeroMQ socket. Finally, the method logs a message indicating that 
  the publisher is successfully connected to the leader.
  """
  @handle_exception
  def register(self, name, topiclist):
    self.logger.info("PublisherMW::register - register publisher to ZK")
    data = {}
    data["id"] = {"id": name, "addr": self.addr, "port": self.port} 
    data["topiclist"] = topiclist
    data_json = json.dumps(data)
    self.zk.create("/publisher/" + self.name, value=data_json.encode("utf-8"), ephemeral=True, makepath=True)
    self.logger.info ("PublisherMW::register - sent register message and now now wait for reply")
  
  @handle_exception
  def setRequest(self):
    self.zk.start()
    while self.zk.exists("/leader") == None:
      time.sleep(1)
    metadata = json.loads(self.zk.get("/leader")[0].decode('utf-8'))
    self.req.connect(metadata["repAddress"])
    self.logger.debug("Successfully connected to the leader")