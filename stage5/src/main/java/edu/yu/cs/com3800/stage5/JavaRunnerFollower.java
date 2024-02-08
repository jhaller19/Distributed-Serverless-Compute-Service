package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread {

	LinkedBlockingQueue<Message> incomingMessages;
	LinkedBlockingQueue<Message> outgoingMessages;
	//boolean shutdown;
	JavaRunner javaRunner;
	ZooKeeperPeerServerImpl myPeerServer;
	Logger logger;
	int tcpPort;
	String hostName;
	ServerSocket serverSocket;
	Socket clientSocket;
	Map<Long, Message> finishedQueuedWorkByRequestId = new HashMap<>();


	public JavaRunnerFollower(LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages, ZooKeeperPeerServerImpl myPeerServer, JavaRunnerFollower prevFollower){
		this.incomingMessages = incomingMessages;
		this.outgoingMessages = outgoingMessages;
		this.myPeerServer = myPeerServer;
		this.hostName = myPeerServer.getAddress().getHostName();
		this.tcpPort = myPeerServer.getUdpPort()+2;
		try {
			this.javaRunner = new JavaRunner();
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.setName("JRF-id-" + myPeerServer.getServerId());
		this.setDaemon(true);
		if(prevFollower == null) {
			this.logger = myPeerServer.initializeLogging("JavaRunnerFollower-ID-" + myPeerServer.getServerId() + "-on-tcp-port-" + this.tcpPort);
		}else{
			this.logger = prevFollower.logger;
		}
		//stage 5
		if(prevFollower != null) finishedQueuedWorkByRequestId.putAll(prevFollower.finishedQueuedWorkByRequestId);
	}

	public void shutdown() throws IOException {
		//this.shutdown = true;
		for(Handler h : logger.getHandlers()){
			h.close();
		}
		logger.severe("Shutting down");
		if(serverSocket!= null)serverSocket.close();
		if(clientSocket != null)clientSocket.close();
		interrupt();

	}

	@Override
	public void run() {
		serverSocket = null;
		try {
			serverSocket = new ServerSocket(this.tcpPort);
		} catch (IOException e) {
			e.printStackTrace();
		}
		while(!this.isInterrupted()) {
			/**STEP 6: Accept TCP connection from leader*/
			InputStream in = null;
			OutputStream out = null;
			try {
				clientSocket = serverSocket.accept();
				//Read Message from connection
				in = clientSocket.getInputStream();
				out = clientSocket.getOutputStream();
			}catch (IOException e){
				//e.printStackTrace();
				return;
			}
			logger.fine("Accepted connection from leader");

			byte[] workMessageAsNetworkPayload = Util.readAllBytesFromNetwork(in);
			Message workMessage = new Message(workMessageAsNetworkPayload);
			if(workMessage.getMessageType() == Message.MessageType.NEW_LEADER_GETTING_LAST_WORK){
				logger.fine("Asked for queued work");
				sendQueuedWork(out, workMessage);
				logger.fine("Finished sending any queued work");
				continue;
			}
			logger.finer("Received new WORK message to take care of: \n" + workMessage.toString());
			//run the code and create new message with output
			String output = null;
			boolean wasError = false;
			try {
				output = javaRunner.compileAndRun(new ByteArrayInputStream(workMessage.getMessageContents()));
				logger.info("Code ran fine. Output: " + output);
			} catch (Exception e) {
				wasError = true;
				ByteArrayOutputStream stackTraceBaos = new ByteArrayOutputStream();
				PrintStream ps = new PrintStream(stackTraceBaos);
				e.printStackTrace(ps);
				String stackTrace = stackTraceBaos.toString();
				output = e.getMessage() + "\n" + stackTrace;
				logger.info("Code threw exception: " + output);
			}
			/**STEP 7: Send work back to leader*/
			//TODO: Make sure code executes here in the case of a catch
			//Create completed work message
			Message completedWorkMessage = new Message(Message.MessageType.COMPLETED_WORK, output.getBytes(), this.hostName,
					this.tcpPort, workMessage.getSenderHost(), workMessage.getSenderPort(), workMessage.getRequestID(), wasError);
			logger.finer("Sending COMPLETED_WORK message back to leader: \n" + completedWorkMessage.toString());
			if(leaderWentDown(workMessage,completedWorkMessage)){
				continue;
			}
			try {
				out.write(completedWorkMessage.getNetworkPayload());
			} catch (IOException e) {
				e.printStackTrace();
			}
			logger.info("Success!");
		}
		try {
			serverSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.warning("Shutting down");

	}
	private boolean leaderWentDown(Message workMessage, Message completedWorkMessage){
		if(myPeerServer.isPeerDead(new InetSocketAddress(workMessage.getSenderHost(),workMessage.getSenderPort()))){
			logger.fine("Leader went down after I finished work. Queueing finished work");
			finishedQueuedWorkByRequestId.put(completedWorkMessage.getRequestID(), completedWorkMessage);
			return true;
		}
		return false;
	}

	private void sendQueuedWork(OutputStream out, Message messageFromLeader){
		//todo im assuming client can only have 1 message queued
		Message queuedWorkBackToLeader = null;
		if(!finishedQueuedWorkByRequestId.isEmpty()){
			for(long id : finishedQueuedWorkByRequestId.keySet()){
				queuedWorkBackToLeader = finishedQueuedWorkByRequestId.get(id);
				logger.fine("Sending queued work to leader" + queuedWorkBackToLeader.toString());
				finishedQueuedWorkByRequestId.remove(id);
			}
			try {
				out.write(queuedWorkBackToLeader.getNetworkPayload());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			logger.fine("Don't have any queued work to send to leader");
			out.write(new Message(Message.MessageType.COMPLETED_WORK, new byte[0] , myPeerServer.getAddress().getHostName(),myPeerServer.myTcpPort, messageFromLeader.getSenderHost(), messageFromLeader.getSenderPort(), -1, true).getNetworkPayload());
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
