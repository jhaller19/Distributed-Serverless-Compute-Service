package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl {

	public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress, int nObservers, int gatewayPort) {
		super(myPort, peerEpoch, id, peerIDtoAddress, nObservers, gatewayPort);
		super.state = ServerState.OBSERVER;
		super.currentLeader = null; //this seems to be ok but not sure

	}

	@Override
	public void run() {
		receiverWorker.start();
		senderWorker.start();
		gossipThread.start();
		//step 3: main server loop
		this.logger.fine("GatewayPeerServer observing election...");
		try{
			while (!this.shutdown){
				//TODO: What happens after election is over?
				if (getPeerState() == ServerState.OBSERVER) {
					//start leader election, set leader to the election winner
					if(this.currentLeader == null) {
						logger.fine("Gateway starting new election");
						this.zooKeeperLeaderElection = new ZooKeeperLeaderElection(this, incomingElectionMessages);
						Vote leader = this.zooKeeperLeaderElection.lookForLeader();
						this.setCurrentLeader(leader);
						this.peerEpoch++;
						this.logger.fine("GatewayPeer set leader to: " + leader.toString());
					}
				} else {
					this.logger.warning("GATEWAY SERVER CHANGED STATE FROM OBSERVING");
				}
			}
		}
		catch (Exception e) {
			//code...
			this.logger.warning("Exception thrown in while loop " + e);
			e.printStackTrace();
		}
	}

	@Override
	public void reportFailedPeer(long peerID) {
		super.reportFailedPeer(peerID);
		if(currentLeader == null){
			return;
		}
		if(currentLeader.getProposedLeaderID() == peerID){
			currentLeader = null;
		}
	}
}
