import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
from topic_selector import TopicSelector
from CS6381_MW.DiscoveryMW import DiscoveryMW
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from enum import Enum  # for an enumeration we are using to describe what state we are in
from functools import wraps # for a decorator we are using to handle exceptions

class DiscoveryAppln():
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        WAIT = 2,
        ISREADY = 3,
        DISSEMINATE = 4,
        COMPLETED = 5
    
    def handle_exception(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise e
        return wrapper
    
    def __init__(self, logger):
        self.state = self.State.INITIALIZE # state that are we in
        self.name = None # our name (some unique name)
        self.topiclist = None # the different topics in the discovery service
        self.iters = None   # number of iterations of publication
        self.frequency = None # rate at which dissemination takes place
        self.num_topics = None # total num of topics in the discovery service
        self.mw_obj = None # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements
        self.no_pubs = 0 # Initialise to 0
        self.no_subs = 0 # Initialise to 0
        self.no_broker = 0 # Initialise to 0
        self.pub_list = [] # Initialise to empty list
        self.sub_list = [] # Initialise to empty list
        self.broker_list = [] # Initalise to empty list
        self.lookup = None # one of the diff ways we do lookup
        self.dissemination = None # direct or via broker
        self.is_ready = False
        self.topics2pubs = {}
        self.pubs2ip = {}
    
    @handle_exception
    def configure(self, args):
        self.logger.info("DiscoveryAppln::configure")
        self.state = self.State.CONFIGURE
        self.name = args.name # our name
        self.iters = args.iters  # num of iterations
        self.frequency = args.frequency # frequency with which topics are disseminated
        self.num_topics = args.num_topics  # total num of topics we publish
        self.no_pubs = args.no_pubs
        self.no_subs = args.no_subs
        self.no_broker = args.no_broker
        config = configparser.ConfigParser()
        config.read(args.config)
        self.lookup = config["Discovery"]["Strategy"]
        self.dissemination = config["Dissemination"]["Strategy"]
        self.mw_obj = DiscoveryMW(self.logger)
        self.mw_obj.configure(args) # pass remainder of the args to the m/w object
        self.logger.info("DiscoveryAppln::configure - configuration complete")

    @handle_exception    
    def driver (self):
        self.logger.info("DiscoveryAppln::driver")
        self.dump()
        self.logger.info("DiscoveryAppln::driver - upcall handle")
        self.mw_obj.setWatch()
        self.mw_obj.set_upcall_handle(self)
        self.state = self.State.ISREADY
        self.mw_obj.event_loop(timeout=0)  # start the event loop
        self.logger.info("DiscoveryAppln::driver completed")

    @handle_exception    
    def register_request(self, reg_request):
        self.logger.info("DiscoveryAppln::register_request")
        status = False # success = True, failure = False
        reason = ""
        if reg_request.role == discovery_pb2.ROLE_PUBLISHER:
            self.logger.info("DiscoveryAppln::register_request - ROLE_PUBLISHER")
            if len(self.pub_list) != 0:
                for pub in self.pub_list:
                    if pub[0] == reg_request.info.id:
                        reason = "The publisher name is not unique."
            if reason == "":
                self.pub_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                status = True
                reason = "The publisher name is unique."
        elif reg_request.role == discovery_pb2.ROLE_SUBSCRIBER:
            self.logger.info("DiscoveryAppln::register_request - ROLE_SUBSCRIBER")
            if len(self.sub_list) != 0:
                for sub in self.sub_list:
                    if sub[0] == reg_request.info.id:
                        reason = "The subscriber name is not unique."
            if reason == "":
                self.sub_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                status = True
                reason = "The subscriber name is unique."
        elif reg_request.role == discovery_pb2.ROLE_BOTH:
            self.logger.info("DiscoveryAppln::register_request - ROLE_BOTH")
            if len(self.broker_list) != 0:
                reason = "There should be only one broker."
            if reason == "":
                self.broker_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                status = True
                reason = "The broker name is unique and there is only one broker."
        else:
            raise Exception("Role unknown: Should be either publisher, subscriber, or broker.")
        if len(self.pub_list) >= self.no_pubs and len(self.sub_list) >= self.no_subs:
            self.is_ready = True
        self.mw_obj.handle_register(status, reason)
        return 0

    @handle_exception
    def isready_request(self):
        self.logger.info("DiscoveryAppln:: isready_request")
        self.mw_obj.update_is_ready_status(True)
        return 0
    
    @handle_exception
    def handle_topic_request(self, topic_req):
        self.logger.info("DiscoveryAppln::handle_topic_request - start")
        pubTopicList = []
        for pub in self.pub_list:
            if any(topic in pub[3] for topic in topic_req.topiclist):
                self.logger.info("DiscoveryAppln::handle_topic_request - add pub")
                pubTopicList.append([pub[0], pub[1], pub[2]])     
        self.mw_obj.send_pubinfo_for_topic(pubTopicList)
        return 0

    @handle_exception    
    def handle_all_publist(self):
        self.logger.info ("DiscoveryAppln:: handle_all_publist")
        pubWithoutTopicList = []
        if len(self.pub_list) != 0:
            for pub in self.pub_list:
                pubWithoutTopicList.append([pub[0], pub[1], pub[2]])
        else:
            pubWithoutTopicList = []
        self.mw_obj.send_all_pub_list(pubWithoutTopicList)
        return 0
    
    # New code for PA3
    """
    invoke_operation(): I think I need for this assignment to check the current state of the 
    application and decides whether to execute a requested operation or not. 
    
    backup(): This method is responsible for sending the state of the application to a replica. 
    It calls the sendStateReplica() method of a DiscoveryMW object (which is an instance variable 
    of the DiscoveryAppln class) and passes the current state information (topics2pubs, pubs2ip, 
    no_pubs, and no_subs) to it.

    setBrokerInfo(self, broker): This method is called by the DiscoveryMW object when it receives 
    information about a new broker. It sets the broker instance variable of the DiscoveryAppln 
    object to the given value.

    setPublisherInfo(self, publishers): This method is called by the DiscoveryMW object when it 
    receives information about publishers. It updates the pubs2ip and topics2pubs instance 
    variables of the DiscoveryAppln object based on the given information.

    setState(self, topics2pubs, pubs2ip, no_pubs, no_subs): This method is called by the 
    DiscoveryMW object when it receives state information from a leader or a replica. It 
    updates the topics2pubs, pubs2ip, no_pubs, and no_subs instance variables of the 
    DiscoveryAppln object based on the given information.
    """
    @handle_exception
    def invoke_operation(self):
        self.logger.info("DiscoveryAppln::invoke_operation - start")
        if self.state == self.State.WAIT or self.state == self.State.ISREADY:
            return None
        else:
            raise ValueError("undefined")
        
    @handle_exception
    def backup(self):
        self.logger.info("DiscoveryAppln::backup - start")
        self.mw_obj.sendStateReplica(self.topics2pubs, self.pubs2ip, self.no_pubs, self.no_subs, self.state)

    @handle_exception
    def setBrokerInfo(self, broker):   
        self.logger.info("DiscoveryAppln::setBrokerInfo - start")
        self.broker = broker
        
    def setPublisherInfo(self, publishers):
        self.logger.info("DiscoveryAppln::setPublisherInfo - updating publishers")
        self.pubs2ip = {}
        self.topics2pubs = {}
        for pub in publishers:
            information = pub["id"]
            topiclist = pub["topiclist"]
            self.pubs2ip[information["id"]] = information
            for topic in topiclist:
                if topic not in self.topics2pubs:
                    self.topics2pubs[topic] = []
                self.topics2pubs[topic].append(information["id"])
        self.logger.info("DiscoveryAppln::setPublisherInfo - updated publishers: {}".format(self.pubs2ip))
        self.logger.info("DiscoveryAppln::setPublisherInfo - updated topics: {}".format(self.topics2pubs))
            
    @handle_exception
    def setState(self, topics2pubs, pubs2ip, no_pubs, no_subs):
        self.topics2pubs = topics2pubs
        self.pubs2ip = pubs2ip
        self.no_pubs = no_pubs
        self.no_subs = no_subs
    
    @handle_exception
    def dump(self):
        self.logger.info ("**********************************")
        self.logger.info ("DiscoveryAppln::dump")
        self.logger.info ("     Lookup: {}".format (self.lookup))
        self.logger.info ("     Dissemination: {}".format (self.dissemination))
        self.logger.info ("**********************************")
 
def parseCmdLineArgs ():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser (description="Discovery Application")
    # Now specify all the optional arguments we support
    parser.add_argument("-n", "--name", default="discovery", help="Discovery")
    parser.add_argument("-r", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")
    parser.add_argument("-t", "--port", type=int, default=5555, help="Port number on which our underlying publisher ZMQ service runs, default=5577")
    parser.add_argument("-P", "--no_pubs", type=int, default=1, help="Number of publishers")
    parser.add_argument("-S", "--no_subs", type=int, default=1, help="Number of subscribers")
    parser.add_argument("-B", "--no_broker", type=int, default=1, help="Number of brokers")
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish, currently restricted to max of 9")
    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    parser.add_argument("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    # New code for PA2
    parser.add_argument ("-j", "--dht_json", default="dht.json", help="JSON file with all DHT nodes, default dht.json")
    # New code for PA3
    parser.add_argument("-q", "--quorum", type=int, default=3, help="Number of discovery nodes in the quorum, default=3")
    parser.add_argument ("-z", "--zookeeper", default="localhost:2181", help="IPv4 address for the zookeeper service, default = localhost:2181")
    return parser.parse_args()
    
def main():
    try:
        logging.info("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("DiscoveryAppln")
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()
        logger.debug("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format (logger.getEffectiveLevel ()))
        logger.debug("Main: obtain the Discovery appln object")
        discovery_app = DiscoveryAppln(logger)
        logger.debug("Main: configure the Discovery appln object")
        discovery_app.configure(args)
        logger.debug("Main: invoke the Discovery appln driver")
        discovery_app.driver()
    except Exception as e:
        logger.error("Exception caught in main - {}".format (e))
        return

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()