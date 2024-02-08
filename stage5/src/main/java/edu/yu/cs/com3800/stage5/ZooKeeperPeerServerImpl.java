package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Handler;
import java.util.logging.Logger;

public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer{



    final InetSocketAddress myAddress;
    final int myUdpPort;
    final int myTcpPort;
    volatile ServerState state;
    volatile boolean shutdown;
    LinkedBlockingQueue<Message> outgoingMessages;
    LinkedBlockingQueue<Message> incomingElectionMessages;
    LinkedBlockingQueue<Message> incomingGossipMessages;
    Long id;
    long peerEpoch;
    volatile Vote currentLeader;
    Map<Long,InetSocketAddress> peerIDtoAddress;
    volatile List<InetSocketAddress> allWorkers;
    UDPMessageSender senderWorker;
    UDPMessageReceiver receiverWorker;
    ZooKeeperLeaderElection zooKeeperLeaderElection;
    Logger logger;
    int nObservers;
    //Stage 5
    Map<Long,GossipData> gossipTable = new HashMap<>();
    GossipThread gossipThread;
    Set<Long> failedPeersByID = new HashSet<>();
    Set<InetSocketAddress> failedPeersByAddress = new HashSet<>();
    Map<InetSocketAddress,Long> addressToPeerId = new HashMap<>();
    ServerState prevState = null;
    JavaRunnerFollower javaRunnerFollower;
    int gatewayPort;
    RoundRobinLeader roundRobinLeader;


    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress, int nObservers, int gatewayPort){
        //code here...
        this.myUdpPort = myPort;
        this.myTcpPort = this.myUdpPort + 2;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = peerIDtoAddress;
        this.myAddress = new InetSocketAddress("localhost" , this.myUdpPort);//NS
        this.peerIDtoAddress.put(this.id , this.myAddress);//NS
        this.currentLeader = null;
        this.state = ServerState.LOOKING;
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingElectionMessages = new LinkedBlockingQueue<>();
        this.incomingGossipMessages = new LinkedBlockingQueue<>();
        this.senderWorker = new UDPMessageSender(this.outgoingMessages , this.myUdpPort);
        try {
            this.receiverWorker = new UDPMessageReceiver(this.incomingElectionMessages,this.incomingGossipMessages, this.myAddress, this.myUdpPort, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //this.zooKeeperLeaderElection = new ZooKeeperLeaderElection(this, this.incomingMessages);
        this.logger = initializeLogging(getClass().getSimpleName() +"-ID-" + this.id + "-on-udp-port-" + this.myUdpPort);
        this.logger.fine("Logger init");
        //stage 5
        this.nObservers = nObservers;
        this.gossipThread = new GossipThread(new ArrayList<>(peerIDtoAddress.values()), gossipTable, this, outgoingMessages, incomingGossipMessages, System.currentTimeMillis());
        popMap();
        this.gatewayPort = gatewayPort;

    }

    @Override
    public void shutdown(){
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        this.gossipThread.shutdown();
        if(this.javaRunnerFollower != null && this.javaRunnerFollower.isAlive()){
            try {
                this.javaRunnerFollower.shutdown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(this.roundRobinLeader != null && this.roundRobinLeader.isAlive()){
            try {
                this.roundRobinLeader.shutdown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.severe("Shutting down");
        for(Handler h : logger.getHandlers()){
            h.close();
        }
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;

    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        //Not sure about strings
        Message message = new Message(type, messageContents, this.myAddress.getHostString(),
                this.myUdpPort, target.getHostString() , target.getPort());

        outgoingMessages.offer(message);
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for(InetSocketAddress target : peerIDtoAddress.values()){
            if(!target.equals(this.myAddress)){
                sendMessage(type,messageContents,target);
            }
        }
    }

    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.myUdpPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return this.peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        return ((peerIDtoAddress.size() - nObservers)/2) + 1; //NS
    }

    @Override
    public void run(){
        //step 1: create and run thread that sends broadcast messages
        //Util.startAsDaemon(senderWorker , "senderWorker");
        //step 2: create and run thread that listens for messages sent to this server
        //Util.startAsDaemon(receiverWorker, "receiverWorker");
       receiverWorker.start();
       senderWorker.start();
       gossipThread.start();

        //step 3: main server loop
        try{
            while (!this.shutdown){
                switch (getPeerState()){
                    case LOOKING:
                        log(ServerState.LOOKING);
                        this.logger.fine("Entered LOOKING block. Starting election");
                        //start leader election, set leader to the election winner
                        this.zooKeeperLeaderElection = new ZooKeeperLeaderElection(this, incomingElectionMessages);
                        Vote leader = this.zooKeeperLeaderElection.lookForLeader();
                        this.setCurrentLeader(leader);
                        this.logger.fine("Set leader to: " + leader.toString());
                        break;
                    case FOLLOWING:
                        log(ServerState.FOLLOWING);
                        logger.fine("Entered FOLLOWING block. Starting JavaRunnerFollower");
                        javaRunnerFollower = new JavaRunnerFollower(incomingElectionMessages,outgoingMessages,this, javaRunnerFollower);
                        javaRunnerFollower.start();
                       // System.out.println("Ready again " + id );
                        while(!shutdown && getPeerState() == ServerState.FOLLOWING && !isPeerDead(getCurrentLeader().getProposedLeaderID())){
                        }
                        if(!shutdown){
                            setPeerState(ServerState.LOOKING);
                            peerEpoch++;
                            currentLeader = null;
                        }
                        javaRunnerFollower.shutdown();
                        break;
                    case LEADING:
                        log(ServerState.LEADING);
                        logger.fine("Entered LEADING block. Starting RoundRobinLeader");
                        roundRobinLeader = new RoundRobinLeader(incomingElectionMessages,outgoingMessages,this, peerIDtoAddress.values(), javaRunnerFollower);
                        roundRobinLeader.start();
                        while(!shutdown && getPeerState() == ServerState.LEADING){}
                        roundRobinLeader.shutdown();
                        break;
                    case OBSERVER:
                        break;
                }
            }
        }
        catch (Exception e) {
            //code...
            this.logger.warning("Exception thrown in while loop " + e);
            e.printStackTrace();
        }
    }
    private void log(ServerState state){
        if(prevState == null){
           prevState = state;
            return;
        }
        logger.fine(id + ": changing stage from " + prevState + " to " + state);
        System.out.println(id + ": changing stage from " + prevState + " to " + state);
        prevState = state;
    }
    protected long getGatewayPort(){
        return gatewayPort;
    }



    @Override
    public void reportFailedPeer(long peerID) {
        failedPeersByID.add(peerID);
        failedPeersByAddress.add(peerIDtoAddress.get(peerID));
        peerIDtoAddress.remove(peerID);
    }

    @Override
    public boolean isPeerDead(long peerID) {
        return failedPeersByID.contains(peerID);
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        return failedPeersByAddress.contains(address);
    }

    static class GossipData{
        long heartbeatCounter;
        long time;
        GossipData(long heartbeatCounter, long time){
            this.heartbeatCounter = heartbeatCounter;
            this.time=time;
        }
        @Override
        public String toString() {
            return "[" + heartbeatCounter + ", " + time + "]";
        }
    }

    private void popMap(){
        for(long id : peerIDtoAddress.keySet()){
            addressToPeerId.put(peerIDtoAddress.get(id) , id);
        }
    }

    public static void main(String[] args) {
        ConcurrentHashMap<Long,InetSocketAddress> map = new ConcurrentHashMap<>();
        int port = 8010;
        int myPort = -1;
        long myId = Integer.parseInt(args[0]);
        for (long i = 1; i <= 7; i++) {
            if(i == myId){
                myPort = port;
                port+=10;
                continue;
            }
            map.put(i, new InetSocketAddress("localhost", port));
            port += 10;
        }
        map.put(8L, new InetSocketAddress("localhost" , port+2));
        ZooKeeperPeerServerImpl zooKeeperPeerServer = new ZooKeeperPeerServerImpl(myPort, 0, myId, map, 1, 8082);
        zooKeeperPeerServer.start();
    }

}