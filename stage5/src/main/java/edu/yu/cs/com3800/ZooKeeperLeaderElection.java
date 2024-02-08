package edu.yu.cs.com3800;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ZooKeeperLeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 60 seconds.
     */
    private final static int maxNotificationInterval = 60000;
    ZooKeeperPeerServer myPeerServer;
    LinkedBlockingQueue<Message> incomingMessages;
    long proposedLeader;
    long proposedEpoch;
    Logger logger;

    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages) {
        this.incomingMessages = incomingMessages;
        this.myPeerServer = server;
        this.proposedEpoch = myPeerServer.getPeerEpoch();//NS
        this.proposedLeader = myPeerServer.getServerId();//NS
        if(myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.OBSERVER){
            this.proposedLeader = -1;
        }
        logger = this.myPeerServer.initializeLogging("ZookeeperLeaderElection-Server-ID-" + myPeerServer.getServerId() + "-on-udp-port-" + myPeerServer.getUdpPort());
    }

    protected static ElectionNotification getNotificationFromMessage(Message received) {
        ByteBuffer msgBytes = ByteBuffer.wrap(received.getMessageContents());
        long leader = msgBytes.getLong();
        char stateChar = msgBytes.getChar();
        long senderID = msgBytes.getLong();
        long peerEpoch = msgBytes.getLong();
        return new ElectionNotification(leader, ZooKeeperPeerServer.ServerState.getServerState(stateChar), senderID, peerEpoch);//todo
    }

    protected static byte[] buildMsgContent(ElectionNotification notification) {
        long proposedLeaderID = notification.getProposedLeaderID();
        char stateChar = notification.getState().getChar();
        long senderID = notification.getSenderID();
        long peerEpoch = notification.getPeerEpoch();
        /*
        size of buffer =
        1 char = 2 bytes
        3 long = 24 bytes
         */
        int bufferSize = 2 + 24;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.putLong(proposedLeaderID);
        buffer.putChar(stateChar);
        buffer.putLong(senderID);
        buffer.putLong(peerEpoch);
        buffer.flip(); //NS
        return buffer.array();
    }


    private synchronized Vote getCurrentVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    private void sendNotifications() {
        //todo
        byte[] contents = ZooKeeperLeaderElection.buildMsgContent(new ElectionNotification(this.proposedLeader,
                myPeerServer.getPeerState(), myPeerServer.getServerId(), this.proposedEpoch));//NS

        myPeerServer.sendBroadcast(Message.MessageType.ELECTION, contents);//NS
    }

    public synchronized Vote lookForLeader() {
        logger.fine("Starting to look for leader (Q: " + myPeerServer.getQuorumSize()+")");
        Map<Long, ElectionNotification> voteCount = new HashMap<>();
        //put my vote in
        int timeOut = finalizeWait * 2;
        //send initial notifications to other peers to get things started
        sendNotifications();
        //Loop, exchanging notifications with other servers until we find a leader
        try {
            while (this.myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.LOOKING || this.myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.OBSERVER) {
                //Remove next notification from queue, timing out after 2 times the termination time
                Message newMessage = incomingMessages.poll(timeOut, TimeUnit.MILLISECONDS);
                //if no notifications received..
                if (newMessage == null) {
                    logger.fine("No messages in queue, send messages, backoff, and try again");
                    //..resend notifications to prompt a reply from others..
                    sendNotifications();
                    //.and implement exponential back-off when notifications not received..
                    int potentialTimeOut = timeOut * 2; //NS
                    timeOut = Math.min(potentialTimeOut, maxNotificationInterval);
                    continue;
                }
                ElectionNotification electionNotification = ZooKeeperLeaderElection.getNotificationFromMessage(newMessage);
                logger.fine("New Message received: " + newMessage.toString());
                //if/when we get a message and it's from a valid server and for a valid server..
                if (electionNotification.getPeerEpoch() < this.proposedEpoch) {
                    //todo maybe add other validation
                    logger.warning("Message is from a non-valid server");
                    continue;
                }
                //If message is from dead node
                if(myPeerServer.isPeerDead(electionNotification.getSenderID())){
                    logger.fine("Received message from dead server");
                }
                //switch on the state of the sender:
                ZooKeeperPeerServer.ServerState senderState = electionNotification.getState();
                switch (senderState) {
                    case LOOKING: //if the sender is also looking
                        logger.fine("Received message is LOOKING case");
                        //if the received message has a vote for a leader which supersedes mine, change my vote and tell all my peers what my new vote is.
                        if (supersedesCurrentVote(electionNotification.getProposedLeaderID(), electionNotification.getPeerEpoch())) {
                            logger.fine("Received vote "+ electionNotification.toString() +" supersedes my own "+ getCurrentVote().toString() +" . Update my vote to it.");
                            this.proposedLeader = electionNotification.getProposedLeaderID();
                            this.proposedEpoch = electionNotification.getPeerEpoch(); //NS
                            sendNotifications();
                        }
                        //keep track of the votes I received and who I received them from.
                        voteCount.put(electionNotification.getSenderID(), electionNotification);
                        ////if I have enough votes to declare my currently proposed leader as the leader:
                        if (haveEnoughVotes(voteCount, this.getCurrentVote())) {
                            logger.fine("My current vote for " + getCurrentVote().toString() + " has a quorum");
                            //first check if there are any new votes for a higher ranked possible leader before I declare a leader. If so, continue in my election loop
                            while ((newMessage = incomingMessages.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                                ElectionNotification lastCheckElectionNotification = getNotificationFromMessage(newMessage);
                                if(myPeerServer.isPeerDead(electionNotification.getSenderID())){
                                    continue;
                                }
                                if (supersedesCurrentVote(lastCheckElectionNotification.getProposedLeaderID(), lastCheckElectionNotification.getPeerEpoch())) {
                                    logger.fine("BUT, there is a better message "+ lastCheckElectionNotification.toString() +" in the queue so loop again");
                                    incomingMessages.put(newMessage);
                                    break;
                                }
                            }
                            //If not, set my own state to either LEADING (if I won the election) or FOLLOWING (if someone else won the election) and exit the election
                            if (newMessage == null) {
                                return acceptElectionWinner(electionNotification);
                            }
                        }
                        break;
                    case FOLLOWING:
                    case LEADING://if the sender is following a leader already or thinks it is the leader
                        logger.fine("Received message is FOLLOWING/LEADING case");
                        //IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in, i.e. it gives the majority to the vote of the FOLLOWING or LEADING peer whose vote I just received.
                        voteCount.put(electionNotification.getSenderID(), electionNotification); //NS
                        if (electionNotification.getPeerEpoch() == this.proposedEpoch) {
                            //if so, accept the election winner.
                            //As, once someone declares a winner, we are done. We are not worried about / accounting for misbehaving peers.
                            if (haveEnoughVotes(voteCount, electionNotification)) {
                                return acceptElectionWinner(electionNotification);
                            }
                        }
                        //ELSE:
                        //if n is from a LATER election epoch
                        else if (electionNotification.getPeerEpoch() > this.proposedEpoch) {
                            //IF a quorum from that epoch are voting for the same peer as the vote of the FOLLOWING or LEADING peer whose vote I just received.
                            //THEN accept their leader, and update my epoch to be their epoch
                            //todo very unsure
                            if (haveEnoughVotes(voteCount, electionNotification)) {
                                this.proposedEpoch = electionNotification.getPeerEpoch();
                                return acceptElectionWinner(electionNotification);
                            }
                        }
                        break;
                    case OBSERVER:
                        logger.fine("Received and ignoring vote from OBSERVING peer");
                        //ELSE:
                        //keep looping on the election loop.
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.warning("Exception thrown " + e);
        }
        return null;//NS
    }

    private Vote acceptElectionWinner(ElectionNotification n) {
        //set my state to either LEADING or FOLLOWING
        logger.fine("Accepted Leader: " + n.toString());
        /*try {
            this.myPeerServer.setCurrentLeader(n); //todo: this prob shouldn't be here for stage 5
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        if(myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.OBSERVER){
            //DONT CHANGE STATE
        }else if (n.getProposedLeaderID() == myPeerServer.getServerId()) {
            myPeerServer.setPeerState(ZooKeeperPeerServer.ServerState.LEADING);
        } else {
            myPeerServer.setPeerState(ZooKeeperPeerServer.ServerState.FOLLOWING);
        }
        this.proposedLeader = n.getProposedLeaderID();
        // epoch change?
        //clear out the incoming queue before returning
        incomingMessages.clear();
        return n;
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        //is the number of votes for the proposal > the size of my peer server’s quorum?
        int count = 0;
        //count this servers current vote
        if(!myPeerServer.getPeerState().equals(ZooKeeperPeerServer.ServerState.OBSERVER) && proposal.getProposedLeaderID() == this.proposedLeader && proposal.getPeerEpoch() == this.proposedEpoch){
            count++;
        }
        for (ElectionNotification e : votes.values()) {
            if(e.getProposedLeaderID() == proposal.getProposedLeaderID() && e.getPeerEpoch() == this.proposedEpoch)count++;
        }
        /*if(count >= myPeerServer.getQuorumSize()){
            System.out.println(count + "**" + myPeerServer.getQuorumSize() + "**" + this.proposedLeader);
        }*/
        return count >= myPeerServer.getQuorumSize();
    }
}