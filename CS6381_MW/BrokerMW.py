import zmq  # ZMQ sockets
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from CS6381_MW.Common import PinguMW
from functools import wraps
import time
import json
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from kazoo.recipe.election import Election
from kazoo.recipe.watchers import DataWatch

class BrokerMW(PinguMW):
    def __init__ (self, logger):
        super().__init__(logger)
        self.req = None # will be a ZMQ REQ socket to talk to Discovery service
        self.pub = None # will be a ZMQ XPUB socket for representing publisher
        self.sub = None # will be a ZMQ XSUB socket for representing publisher
        
    def handle_exception(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise e
        return wrapper
    
    @handle_exception
    def configure (self, args):
        self.logger.info("BrokerMW::configure")
        self.port = args.port
        self.addr = args.addr
        context = zmq.Context()  # returns a singleton object
        self.poller = zmq.Poller()
        self.req = context.socket(zmq.REQ)
        self.pub = context.socket(zmq.XPUB)
        self.sub = context.socket(zmq.XSUB)
        self.poller.register(self.req, zmq.POLLIN)
        self.poller.register(self.sub, zmq.POLLIN)
        connect_str = "tcp://" + args.discovery
        self.req.connect(connect_str)
        bind_string = "tcp://*:" + str(self.port)
        self.pub.bind(bind_string)
        self.logger.info("BrokerMW::configure completed")
        
    # run the event loop where we expect to receive a reply to a sent request
    def event_loop(self, timeout=None):
        super().event_loop("BrokerMW", self.req, timeout)
    
    @handle_exception
    def handle_reply(self):
        self.logger.info("BrokerMW::handle_reply")
        bytesRcvd = self.req.recv()
        discovery_response = discovery_pb2.DiscoveryResp()
        discovery_response.ParseFromString(bytesRcvd)
        if (discovery_response.msg_type == discovery_pb2.TYPE_REGISTER):
            timeout = self.upcall_obj.register_response(discovery_response.register_resp)
        elif (discovery_response.msg_type == discovery_pb2.TYPE_ISREADY):
            timeout = self.upcall_obj.isready_response(discovery_response.isready_resp)
        elif (discovery_response.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
            timeout = self.upcall_obj.allPublishersResponse(discovery_response.allpubs_resp)
        else: 
            raise ValueError ("Unrecognized response message")
        return timeout
    
    def register(self, name, topiclist):
        super().register("BrokerMW", name, topiclist)
    
    def is_ready(self):
        super().is_ready("BrokerMW")
    
    # here we save a pointer (handle) to the application object
    def set_upcall_handle(self, upcall_obj):
        super().set_upcall_handle(upcall_obj)
        
    def disable_event_loop(self):
        super().disable_event_loop()
    
    @handle_exception 
    def receive_msg_sub(self):
        self.logger.info("BrokerMW::recv_msg_sub - receive messages")
        msg = self.sub.recv_string()
        self.logger.info("BrokerMW::recv_msg_sub - received message = {}".format (msg))               
        return msg 
    
    @handle_exception
    def send_msg_pub(self, send_str):
        self.logger.info("BrokerMW::send_msg_pub - disseminate messages to subscribers from broker")
        self.logger.info("BrokerMW::send_msg_pub - {}".format (send_str))
        self.pub.send(bytes(send_str, "utf-8"))

    @handle_exception
    def receiveAllPublishers(self):
        self.logger.info("BrokerMW::receiveAllPublishers - start")
        allpubs_request = discovery_pb2.LookupAllPubsReq()
        discovery_request = discovery_pb2.DiscoveryReq()
        discovery_request.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
        discovery_request.allpubs_req.CopyFrom(allpubs_request)
        buf2send = discovery_request.SerializeToString()
        self.req.send(buf2send) 
        self.logger.info("BrokerMW::receiveAllPublishers - end")

    @handle_exception    
    def connect2pubs(self, IP, port):
        connect_str = "tcp://" + IP + ":" + str(port)
        self.logger.info("BrokerMW:: connect2pubs method. connect_str = {}".format(connect_str))
        self.sub.connect(connect_str)
        
    # New code for PA3
    """
    setRequest() method sets up the connection to the leader, which is responsible for handling 
    client requests. It first waits for the existence of the leader node in the ZooKeeper cluster. 
    When it exists, it retrieves the metadata of the leader node and connects the request socket to 
    the address of the leader.

    setWatch() method sets up watches for the changes of the /broker and /leader nodes in the 
    ZooKeeper cluster. If the /broker node is deleted, it calls the brokerLeader() method to try 
    to become the leader. If the /leader node changes, it calls the setRequest() method to update 
    the connection to the leader. It also sets up a watch for the changes of the /publisher node, 
    which is used to update the list of available publishers.

    brokerLeader() method is responsible for creating a broker node in the ZooKeeper cluster if 
    it does not exist. It sets the value of the node to the JSON encoded string of the broker's 
    address information.

    subscribe() method subscribes to a list of available publishers. It connects the subscriber 
    socket to each publisher's address to start receiving messages.
    """
    @handle_exception
    def setRequest(self):
        while self.zk.exists("/leader") == None:
            time.sleep(1)
        meta = json.loads(self.zk.get("/leader")[0].decode('utf-8'))
        if self.discovery != None:
            self.logger.info("SubscriberMW::set_req: disconnecting from {}".format(self.discovery))
            self.req.disconnect(self.discovery)
        self.req.connect(meta["repAddress"])
        self.discovery = meta["repAddress"]
        self.logger.info("BrokerMW::set_req: connected to {}".format(self.discovery))

    @handle_exception
    def setWatch(self):
        @self.zk.DataWatch("/broker")
        def watchBroker(data, stat):
            if data is None:
                self.logger.info("BrokerMW::watchBroker: broker node has been deleted. Trying to become leader")
                self.brokerLeader()
                
        @self.zk.DataWatch("/leader")
        def watchLeader(data, stat):
            self.logger.info("BrokerMW::watchLeader: leader node changed")
            self.setRequest()
        
        @self.zk.ChildrenWatch("/publisher")
        def watchPublishers(children):
            self.logger.info("BrokerMW::watchPublishers: publishers changed, re-subscribing")
            publishers = []
            for c in children:
                path = "/publisher/" + c
                data, _ = self.zk.get(path)
                publishers.append(json.loads(data.decode("utf-8"))['id'])
            self.logger.info("BrokerMW::watch_pubs: {}".format(publishers))
            self.subscribe(publishers)

    @handle_exception
    def brokerLeader(self, name):
        self.zk.start()
        self.logger.info("BrokerMW::brokerLeader: connected to zookeeper")
        try:
            self.logger.info("BrokerMW::brokerLeader: broker node does not exist, creating self")
            addr = {"id": name, "addr": self.addr, "port": self.port}
            data = json.dumps(addr)
            self.zk.create("/broker", value=data.encode('utf-8'), ephemeral=True, makepath=True)
        except NodeExistsError:
            self.logger.info("BrokerMW::brokerLeader: broker node exists")
    
    @handle_exception    
    def subscribe(self, publist):
        self.logger.info("BrokerMW::subscribe")
        for pub in publist:
            addr = "tcp://" + pub['addr'] + ":" + str(pub['port'])
            self.logger.info("BrokerMW::subscribe: subscribing to {}".format(addr))
            self.sub.connect(addr)