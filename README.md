# CS6381_PA3_M3_Youngjae_Moon
PA3 Milestone3 Youngjae Moon

### What I have done
Milestone 1 & 2 bascially almost done

For Milestone 3,
Do not know how to test it out and make end-to-end time measurements 

### Description of codes

1. exp_generator.py -> Shared by Ethan Nguyen in the Slack
2. Zookeeper.py -> exemplar code to connect with discovery service
3. Zk.py -> some additional codes for pub/sub topic-based model 
4. Wrapped with handle_exception for all functions
5. Updated DiscoveryAppln and BrokerAppln -> does not use isready anymore
6. New functions for PublisherMW -> register, setRequest
7. New functions for SubscriberAppln -> setPublishersInfo
8. New functions for For SubscriberMW -> setRequest, setWatch, writeToCSV
9. New functions for DiscoveryAppln -> backup, setBrokerInfo, setPublisherInfo, setState
10. New functions for DiscoveryMW -> assureQuorum, createLeader, watchLeader, watchBroker, waitBroker, sendStateReplica
11. New functions for BrokerAppln -> setSubscription
12. New functions for BrokerMW -> setRequest, setWatch, brokerLeader, subscribe
13. Added comments for descriptions for these functions for each file.
