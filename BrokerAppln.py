import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
from topic_selector import TopicSelector
from CS6381_MW.BrokerMW import BrokerMW
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from enum import Enum  # for an enumeration we are using to describe what state we are in
from functools import wraps # for a decorator we are using to handle exceptions

class BrokerAppln ():
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        CHECKMSG = 4,
        RECEIVEANDDISSEMINATE = 5,
        COMPLETED = 6
    
    def handle_exception(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise e
        return wrapper
    
    def __init__ (self, logger):
        self.state = self.State.INITIALIZE # state that are we in
        self.name = None # our name (some unique name)
        self.topiclist = None # the different topics that we publish on
        self.iters = None   # number of iterations of publication
        self.frequency = None # rate at which dissemination takes place
        self.mw_obj = None # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements
        self.msg_list = [] # Initalise to empty list
        self.lookup = None # one of the diff ways we do lookup
        self.dissemination = None # direct or via broker
        self.is_ready = None
    
    @handle_exception
    def configure(self, args):
        self.logger.info("BrokerAppln::configure")
        self.state = self.State.CONFIGURE
        self.name = args.name # our name
        self.iters = args.iters  # num of iterations
        self.frequency = args.frequency # frequency with which topics are disseminated
        config = configparser.ConfigParser()
        config.read(args.config)
        self.lookup = config["Discovery"]["Strategy"]
        self.dissemination = config["Dissemination"]["Strategy"]
        self.mw_obj = BrokerMW(self.logger)
        self.mw_obj.configure(args) # pass remainder of the args to the m/w object
        self.topiclist = ["weather", "humidity", "airquality", "light", "pressure", "temperature", "sound", "altitude", "location"] # Subscribe to all topics
        self.logger.info("BrokerAppln::configure - configuration complete")

    @handle_exception    
    def driver(self):
        self.logger.info("BrokerAppln::driver")
        self.dump()
        self.logger.info("BrokerAppln::driver - upcall handle")
        self.mw_obj.set_upcall_handle(self)
        self.mw_obj.setWatch()
        self.state = self.State.REGISTER
        self.mw_obj.event_loop(timeout=0)  # start the event loop
        self.logger.info("BrokerAppln::driver completed")
    
    @handle_exception    
    def invoke_operation(self):
        self.logger.info("BrokerAppln::invoke_operation - state: {}".format(self.state))
        if self.state == self.State.REGISTER:
            return None
        elif self.state == self.State.ISREADY:
            return None
        elif self.state == self.State.CHECKMSG:
            return None

    # New code for PA3
    @handle_exception
    def setSubscription(self, publist):
        self.logger.info("BrokerAppln::setSubscription")
        self.mw_obj.subscribe(publist)
        
    """
    @handle_exception
    def register_response(self, reg_resp):
        self.logger.info ("BrokerAppln::register_response")
        if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
            self.logger.info("BrokerAppln::register_response - registration is a success")
            self.state = self.State.ISREADY  
            return 0  
        else:
            self.logger.info("BrokerAppln::register_response - registration is a failure with reason {}".format (reg_resp.reason))
            raise ValueError ("Broker needs to have unique id")
        
    # handle isready response method called as part of upcall
    # Also a part of upcall handled by application logic
    @handle_exception
    def isready_response(self, isready_resp):
        self.logger.info("BrokerAppln::isready_response")
        if not isready_resp.status:
            self.logger.info("BrokerAppln::driver - Not ready yet; check again")
            time.sleep(10)  # sleep between calls so that we don't make excessive calls
        else:
            # we got the go ahead + set the state to CHECKMSG
            self.state = self.State.CHECKMSG
        return 0

    @handle_exception
    def allPublishersResponse(self, check_response):
        self.logger.info("BrokerAppln::allPublishersResponse")
        for pub in check_response.publist:
            self.logger.info("tcp://{}:{}".format(pub.addr, pub.port))
            self.mw_obj.connect2pubs(pub.addr, pub.port)
        self.state = self.State.RECEIVEANDDISSEMINATE #RECEIVEFROMPUB
        return 0
    """
    
    @handle_exception
    def dump(self):
        self.logger.info("**********************************")
        self.logger.info("BrokerAppln::dump")
        self.logger.info("     Name: {}".format (self.name))
        self.logger.info("     Number of nodes in distributed hash table: {}".format(self.M))
        self.logger.info("     Lookup: {}".format (self.lookup))
        self.logger.info("     Dissemination: {}".format (self.dissemination))
        self.logger.info("     Iterations: {}".format (self.iters))
        self.logger.info("     Frequency: {}".format (self.frequency))
        super().dumpFingerTable()
        self.logger.info ("**********************************")

# Parse command line arguments
def parseCmdLineArgs ():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Broker Application")
    # Now specify all the optional arguments we support
    parser.add_argument("-n", "--name", default="broker", help="broker")
    parser.add_argument("-a", "--addr", default="localhost", help="IP addr of this broker to advertise (default: localhost)")
    parser.add_argument("-p", "--port", type=int, default=5578, help="Port number on which our underlying Broker ZMQ service runs, default=5576")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default dht.json") # changed to dht.json from localhost:5555
    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    parser.add_argument("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    # New code for PA2
    parser.add_argument ("-j", "--dht_json", default="dht.json", help="JSON file with all DHT nodes, default dht.json")
    # New code for PA3
    parser.add_argument ("-z", "--zookeeper", default="localhost:2181", help="IPv4 address for the zookeeper service, default = localhost:2181")
    return parser.parse_args()

def main():
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("BrokerAppln")
        # first parse the arguments
        logger.debug ("Main: parse command line arguments")
        args = parseCmdLineArgs()
        # reset the log level to as specified
        logger.debug("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format (logger.getEffectiveLevel ()))
        # Obtain a Broker application
        logger.debug("Main: obtain the Broker appln object")
        broker_app = BrokerAppln (logger)
        # configure the object
        logger.debug("Main: configure the Broker appln object")
        broker_app.configure(args)
        # now invoke the driver program
        logger.debug("Main: invoke the Broker appln driver")
        broker_app.driver()

    except Exception as e:
        logger.error ("Exception caught in main - {}".format (e))
        return

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()