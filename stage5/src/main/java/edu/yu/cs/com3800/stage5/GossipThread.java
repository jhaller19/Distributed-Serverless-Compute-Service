package edu.yu.cs.com3800.stage5;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl.GossipData;

public class GossipThread extends Thread{

	static final int GOSSIP = 3000;
	static final int FAIL = GOSSIP * 10;
	static final int CLEANUP = FAIL * 2;

	List<InetSocketAddress> liveServers;
	Set<InetSocketAddress> liveServersSet;
	Map<Long,Long> markedServers;

	int nextToGossipTo;
	Map<Long, GossipData> gossipTable;
	ZooKeeperPeerServerImpl server;
	LinkedBlockingQueue<Message> outgoingMessages;
	LinkedBlockingQueue<Message> incomingGossipMessages;
	long startTime;
	long lastGossipTime = -1;
	Logger logger;
	List<MessageForLogging> loggingMessageList = new ArrayList<>();
	HttpServer httpServer;
	Logger allGossipMessagesLogger;


	public GossipThread(List<InetSocketAddress> liveServers,Map<Long,GossipData> gossipTable, ZooKeeperPeerServerImpl server,
						LinkedBlockingQueue<Message> outgoingMessages, LinkedBlockingQueue<Message> incomingGossipMessages, long startTime){
		this.liveServers = new ArrayList<>(liveServers);
		Collections.shuffle(this.liveServers);
		this.startTime = startTime;
		this.gossipTable = gossipTable;
		this.gossipTable.put(server.getServerId(), new GossipData(0, getCurrentTime()));
		this.server = server;
		liveServers.remove(server.getAddress());
		this.outgoingMessages = outgoingMessages;
		this.incomingGossipMessages = incomingGossipMessages;
		this.markedServers = new HashMap<>();
		logger = server.initializeLogging("GossipThread-Server-id-"+server.getServerId()+"-on-udp-port-"+server.myUdpPort);
		setDaemon(true);
		startHttpService();
		setName("GossipThreadID-" + server.id);
		allGossipMessagesLogger = server.initializeLogging("Gossip-Messages-Server-id-"+ server.getServerId() + "-on-udp-port-" + server.myUdpPort);
	}


	@Override
	public void run() {
		while(!isInterrupted()){
			waitTGossip();
			//System.out.println( "                        " + this.server.id + ": " +gossipTable);
			sendGossipMessage();
			receiveGossipMessage();
			markDeadServers();
			cleanup();
		}
	}
	private void cleanup(){
		for(long id : markedServers.keySet()){
			if(getCurrentTime() - markedServers.get(id) > CLEANUP){
				liveServers.remove(server.getPeerByID(id));
				logger.finest("Cleaning up marked server id" + id);
			}
		}
	}
	private void markDeadServers(){
		for (Iterator<Long> it = gossipTable.keySet().iterator(); it.hasNext(); ) {
			long id = it.next();
			if(getCurrentTime() - gossipTable.get(id).time >= FAIL){
				//Found dead node
				System.out.println(this.server.id + ": no heartbeat from server " +id +" – server failed");
				logger.fine("Marking dead server id " + id);
				server.reportFailedPeer(id);
				it.remove(); //todo: concurrent modification
				markedServers.put(id,getCurrentTime());
			}
		}
	}

	private void receiveGossipMessage(){
		Message received = incomingGossipMessages.poll();
		if(received != null){
			addToListOfReceivedMessages(received);
			logger.fine("Received gossip message: \n" + received.toString());
			if(server.isPeerDead(new InetSocketAddress(received.getSenderHost(),received.getSenderPort()))){
				logger.fine("Message was from dead server...ignoring");
				return;
			}
			Map<Long, Long> receivedTable = getGossipTableFromByteArray(received.getMessageContents());
			for(long id : receivedTable.keySet()){
				//If he's marked, ignore
				if(markedServers.containsKey(id)){
					logger.fine("Message was about a server that was already marked dead...ignoring");
					continue;
				}
				//Never seen this entry
				if(!gossipTable.containsKey(id)){
					long curTime = getCurrentTime();
					gossipTable.put(id, new GossipData(receivedTable.get(id), curTime));
					logger.fine(this.server.id + ": updated id #"+ id + "'s heartbeat sequence to"+ receivedTable.get(id)+ "based on message from " + server.addressToPeerId.get(new InetSocketAddress(received.getSenderHost(),received.getSenderPort())) + "at node time "+ curTime);
				}
				//Seen this entry, check if we need to update the table
				else{
					if(gossipTable.get(id).heartbeatCounter < receivedTable.get(id)){
						gossipTable.get(id).heartbeatCounter = receivedTable.get(id);
						gossipTable.get(id).time = getCurrentTime();
					}
				}
			}
			logger.finest("Updated gossip table: " + gossipTable);

		}

	}
	private void addToListOfReceivedMessages(Message message){
		loggingMessageList.add(new MessageForLogging(message));
		allGossipMessagesLogger.fine(new MessageForLogging(message).toString());

	}

	private void waitTGossip(){
		if(lastGossipTime != -1) {
			while (getCurrentTime() - lastGossipTime < GOSSIP) {
			}
		}
		lastGossipTime = getCurrentTime();
	}

	private void sendGossipMessage(){
		//incr my heartbeat
		incrementMyHeartbeat();
		//Send
		InetSocketAddress receiverAddress = liveServers.get(nextToGossipTo %liveServers.size());
		if(receiverAddress.equals(server.myAddress)){
			nextToGossipTo++;//todo nned to test this
			receiverAddress = liveServers.get(nextToGossipTo %liveServers.size());
		}
		nextToGossipTo++;
		Message gossipMessage = new Message(Message.MessageType.GOSSIP, getGossipTableAsByteArray(),
				server.myAddress.getHostName(), server.myUdpPort, receiverAddress.getHostName(),
				receiverAddress.getPort());
		logger.fine("Sending gossip message: \n" + gossipMessage.toString() +"\n My table: "+ gossipTable);
		outgoingMessages.offer(gossipMessage);



	}
	private void incrementMyHeartbeat(){
		GossipData myData= gossipTable.get(server.getServerId());
		myData.heartbeatCounter++;
		myData.time = getCurrentTime();

	}

	public void shutdown(){
		interrupt();
	}

	private byte[] getGossipTableAsByteArray()  {
		int size = gossipTable.size()*2*8;
		ByteBuffer byteBuffer = ByteBuffer.allocate(size);
		for(long id : gossipTable.keySet()){
			byteBuffer.putLong(id);
			byteBuffer.putLong(gossipTable.get(id).heartbeatCounter);
		}
		byteBuffer.flip();
		byte[] ar = byteBuffer.array();
		return ar;
		/*ByteArrayOutputStream byteOut = null;

		try {
			byteOut = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(byteOut);
			out.writeObject(this.gossipTable);
		}catch (IOException e){
			e.printStackTrace();
		}
		return byteOut.toByteArray();*/
	}
	private Map<Long,Long> getGossipTableFromByteArray(byte[] byteOut) {
		Map<Long, Long> table = new HashMap<>();
		ByteBuffer buffer = ByteBuffer.wrap(byteOut);
		buffer.clear();
		while(buffer.hasRemaining()){
			table.put(buffer.getLong(),buffer.getLong());
		}
		return table;
		/*try {
			ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut);
			ObjectInputStream in = new ObjectInputStream(byteIn);
			table = (Map<Long, GossipData>) in.readObject();
		}catch (IOException | ClassNotFoundException e){

		}
		return table;*/

	}

	private long getCurrentTime(){
		return System.currentTimeMillis() - startTime;
	}

	private void startHttpService(){
		try {
			httpServer = HttpServer.create(new InetSocketAddress(server.myUdpPort), 0);
		} catch (IOException e) {
			System.out.println(server.myUdpPort);
			e.printStackTrace();
		}
		httpServer.createContext("/gossipMessages", new GossipServiceHandler());
		httpServer.setExecutor(null);
		httpServer.start();

	}
	class GossipServiceHandler implements HttpHandler {
		@Override
		public void handle(HttpExchange exchange) throws IOException {
			byte[] res = loggingMessageList.toString().getBytes();
			exchange.sendResponseHeaders(200 , res.length);
			exchange.getResponseBody().write(loggingMessageList.toString().getBytes());
		}
	}

	class MessageForLogging{
		long senderId;
		byte[] contents;
		long timeReceived;
		MessageForLogging(Message message){
			try {
				this.senderId = server.addressToPeerId.get(new InetSocketAddress(message.getSenderHost(), message.getSenderPort()));
			}catch (NullPointerException e){
				System.out.println(server.id);
				System.out.println(server.addressToPeerId);
				System.out.println(message.getSenderPort());
			}
			this.contents =message.getMessageContents();
			this.timeReceived = getCurrentTime();
		}

		@Override
		public String toString() {
			StringBuilder b = new StringBuilder();
			b.append("[Sender ID: ").append(senderId).append("\nTime Received: ").append(timeReceived).append("\nContents:");
			b.append("\nID | Heartbeat\n");
			ByteBuffer buffer = ByteBuffer.wrap(contents);
			while(buffer.hasRemaining()){
				b.append(buffer.getLong()).append("      ").append(buffer.getLong()).append("\n");
			}
			return b.toString();
		}
	}


}
