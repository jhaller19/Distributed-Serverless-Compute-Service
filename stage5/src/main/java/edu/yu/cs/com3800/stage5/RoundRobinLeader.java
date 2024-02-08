package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Handler;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread {
	LinkedBlockingQueue<Message> incomingMessages;
	LinkedBlockingQueue<Message> outgoingMessages;
	//boolean shutdown;
	AtomicInteger nextWorkerToSendTo = new AtomicInteger(0);
	ZooKeeperPeerServerImpl myPeerServer;
	Map<Long, InetSocketAddress> requestIdToClient;
	List<InetSocketAddress> listOfWorkers;
	String myHostName;
	int udpPort;
	int tcpPort;
	Logger logger;
	InetSocketAddress gatewayAddress;
	Map<Long,Message> alreadyFinishedWork = new HashMap<>();
	Socket clientSocket;
	ServerSocket serverSocket;


	public RoundRobinLeader(LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages,
							ZooKeeperPeerServerImpl myPeerServer, Collection<InetSocketAddress> listOfAllPeers, JavaRunnerFollower prevFollower){
		this.incomingMessages = incomingMessages;
		this.outgoingMessages = outgoingMessages;
		this.myPeerServer = myPeerServer;
		this.requestIdToClient = new HashMap<>();
		List<InetSocketAddress> allWorkers = new ArrayList<>(listOfAllPeers);
		allWorkers.remove(myPeerServer.getAddress());
		this.listOfWorkers = Collections.synchronizedList(allWorkers);
		this.myHostName = myPeerServer.getAddress().getHostName();
		this.udpPort = myPeerServer.getUdpPort();
		this.setDaemon(true);
		this.tcpPort = this.udpPort + 2;
		logger = myPeerServer.initializeLogging("RoundRobbinLeader-ID-" + myPeerServer.getServerId()+ "-on-tcp-port-" + this.tcpPort);
		logger.fine("Logger init");
		//stage 5
		if(prevFollower != null){
			try {
				Thread.sleep(GossipThread.GOSSIP * 3);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.fine("Gathering work");
			//System.out.println("GW");
			alreadyFinishedWork.putAll(prevFollower.finishedQueuedWorkByRequestId);
			gatherAllFinishedWork();
			logger.fine("Finished gathering work");
			//System.out.println("Fin GW");
		}

	}
	private void gatherAllFinishedWork(){
		for(InetSocketAddress worker : listOfWorkers){
			//todo currently asking gateway bc I dont know who gateway is
			if(!myPeerServer.isPeerDead(worker)){
				if(worker.getPort() == myPeerServer.getGatewayPort()){
					continue;
				}
				logger.fine("Asking " + worker + " for queued work");
				sendGetLastWorkMessage(worker);
				logger.fine("Got queued work from " + worker);
			}
		}
	}

	public void shutdown() throws IOException {
		for(Handler h : logger.getHandlers()){
			h.close();
		}
		logger.severe("Shutting down");
		if(clientSocket != null) clientSocket.close();
		interrupt();
	}
	AtomicInteger communicateWithWorkerThreadCount = new AtomicInteger(1);
	AtomicLong nextRequestID = new AtomicLong(1);
	class CommunicateWithWorkerThread implements Runnable{
		Message messageFromGateway;
		OutputStream gatewayServerTCPOutputStream;
		ThreadLocal<Logger> threadLocalLogger;

		CommunicateWithWorkerThread(Message messageFromGateway, OutputStream gatewayServerTCPOutputStream){
			this.messageFromGateway = messageFromGateway;
			this.gatewayServerTCPOutputStream = gatewayServerTCPOutputStream;
			this.threadLocalLogger = new ThreadLocal<>();
		}
		@Override
		public void run() {
			if (threadLocalLogger.get() == null) {
				threadLocalLogger.set(myPeerServer.initializeLogging("CommunicateWithWorkerThread-" + communicateWithWorkerThreadCount.getAndIncrement()));
			}
			Logger communicateWithWorkerThreadLogger = threadLocalLogger.get();
			boolean overallSuccess = false;
			byte[] completedWorkNetworkPayload = null;
			while(!overallSuccess) {
				InetSocketAddress workerAddress = getNextWorker();
				try (
						/** STEP 5: Establish TCP connection with worker and send work*/
						//TODO: Make sure port number is right
						//Establish TCP connection
						Socket socket = new Socket(workerAddress.getHostName(), workerAddress.getPort() + 2);
						InputStream is = socket.getInputStream();
						OutputStream out = socket.getOutputStream();
				) {

					communicateWithWorkerThreadLogger.fine(Thread.currentThread().getName() + " Established TCP connection with worker" + workerAddress);
					//logger.fine(Thread.currentThread().getName() + " Established TCP connection with worker");
					//Send work to worker
					Message messageToSendToWorker = new Message(Message.MessageType.WORK, messageFromGateway.getMessageContents(),
							myHostName, tcpPort, workerAddress.getHostName(), workerAddress.getPort() + 2, nextRequestID.getAndIncrement());
					out.write(messageToSendToWorker.getNetworkPayload());
					communicateWithWorkerThreadLogger.finer(Thread.currentThread().getName() + " Sent WORK message to worker: \n" + messageToSendToWorker.toString());
					boolean successWithThisFollower = false;
					while (!successWithThisFollower) {
						if (myPeerServer.isPeerDead(workerAddress)) {
							communicateWithWorkerThreadLogger.warning("Follower " + workerAddress + "was found to be down after receiving work...Sending to new follower");
							socket.close();
							break;
						}
						/**STEP 8: Receive finished work from worker*/
						//Synchronously wait for work to be completed by worker
						completedWorkNetworkPayload = Util.leaderReadAllBytes(is);
						if (completedWorkNetworkPayload != null) successWithThisFollower = true;
					}
					if (completedWorkNetworkPayload != null) {
						communicateWithWorkerThreadLogger.finest("Got work back!");
						overallSuccess = true;
					}
					//Close the connection
					socket.close();
				} catch (IOException e) {
					communicateWithWorkerThreadLogger.warning("Exception thrown when communicating with " + workerAddress +". Trying a different worker...");
					//e.printStackTrace();
				}
			}

			/**STEP 9: Send finished work back to gateway*/
			Message completedWorkMessage = new Message(completedWorkNetworkPayload);
			communicateWithWorkerThreadLogger.finer(Thread.currentThread().getName() + " Received COMPLETED_WORK message from worker: \n" + completedWorkMessage.toString());
			//logger.info(Thread.currentThread().getName() + " Received COMPLETED_WORK message from worker: \n" + completedWorkMessage.toString());
			Message messageToSendToGateway = new Message(Message.MessageType.COMPLETED_WORK, completedWorkMessage.getMessageContents(),
					myHostName, tcpPort, messageFromGateway.getSenderHost(), messageFromGateway.getSenderPort(),
					completedWorkMessage.getRequestID(), completedWorkMessage.getErrorOccurred());
			try {
				gatewayServerTCPOutputStream.write(messageToSendToGateway.getNetworkPayload());
			} catch (IOException e) {
				e.printStackTrace();
			}
			communicateWithWorkerThreadLogger.finer(Thread.currentThread().getName() + " Sent COMPLETED_WORK message back to Gateway: \n" + messageToSendToGateway.toString());
			//logger.info(Thread.currentThread().getName() + " Sent COMPLETED_WORK message back to Gateway: \n" + messageToSendToGateway.toString());

		}
	}

	@Override
	public void run() {
		try {
			serverSocket = new ServerSocket(this.tcpPort);
			ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
			while (!isInterrupted()) {
				/**STEP 4: Accept tcp connection from gateway*/
				clientSocket = serverSocket.accept();
				logger.fine("Accepted a connection with Gateway");
				//Read Message from connection
				InputStream in = clientSocket.getInputStream();
				OutputStream out = clientSocket.getOutputStream();
				byte[] messageReceivedFromGatewayAsNetworkPayload = Util.readAllBytesFromNetwork(in);
				Message messageFromGateway = new Message(messageReceivedFromGatewayAsNetworkPayload);
				logger.finer("Received work message from gateway server \n" + messageFromGateway.toString());
				if(this.gatewayAddress == null){
					this.gatewayAddress = new InetSocketAddress(messageFromGateway.getSenderHost() , messageFromGateway.getSenderPort() + 2);
					listOfWorkers.remove(this.gatewayAddress);
				}
				//IF we already have the finished work, send it
				if(alreadyFinishedWork.containsKey(messageFromGateway.getRequestID())){
					out.write(alreadyFinishedWork.get(messageFromGateway.getRequestID()).getNetworkPayload());
					alreadyFinishedWork.remove(messageFromGateway.getRequestID());
				}
				//Else send to worker for processing
				else {
					//start a new thread that will communicate synchronously with
					//a worker and then send the worker’s response back to the gateway
					threadPoolExecutor.execute(new CommunicateWithWorkerThread(messageFromGateway, out));
				}

				//TODO: Clientsocket.close
			}
		}catch (IOException e){
			e.printStackTrace();
			logger.warning("Exception thrown");
		}


	}
	private synchronized InetSocketAddress getNextWorker(){
		InetSocketAddress nextWorkerAddress = listOfWorkers.get((nextWorkerToSendTo.get() % listOfWorkers.size()));
		nextWorkerToSendTo.getAndIncrement();
		while(nextWorkerAddress.equals(gatewayAddress) || myPeerServer.isPeerDead(nextWorkerAddress)){
			nextWorkerAddress = listOfWorkers.get(nextWorkerToSendTo.get() % listOfWorkers.size());
			nextWorkerToSendTo.getAndIncrement();
		}
		logger.finest("Got address of worker to send work to: " + nextWorkerAddress.toString() + " on tcp port " + (nextWorkerAddress.getPort() + 2));
		return nextWorkerAddress;
	}
	private void sendGetLastWorkMessage(InetSocketAddress workerAddress){
		Message getLastWorkMessage = new Message(Message.MessageType.NEW_LEADER_GETTING_LAST_WORK, "lorem ipsum".getBytes(), myHostName, tcpPort, workerAddress.getHostName(), workerAddress.getPort() + 2);
		boolean success = false;
		while(!success) {
			try (
					Socket socket = new Socket(workerAddress.getHostName(), workerAddress.getPort() + 2);
					InputStream is = socket.getInputStream();
					OutputStream out = socket.getOutputStream();
			) {
				out.write(getLastWorkMessage.getNetworkPayload());
				byte[] alreadyCompletedWorkNetworkPayload = Util.readAllBytesFromNetwork(is);
				socket.close();
				Message alreadyCompletedWorkMessage = new Message(alreadyCompletedWorkNetworkPayload);
				if (alreadyCompletedWorkMessage.getMessageContents().length == 0 && alreadyCompletedWorkMessage.getRequestID() == -1 && alreadyCompletedWorkMessage.getErrorOccurred()){
					logger.fine("Worker "+ workerAddress + " had no queued work");
					return;
				}
				alreadyFinishedWork.put(alreadyCompletedWorkMessage.getRequestID(), alreadyCompletedWorkMessage);
				success = true;
			} catch (IOException e) {
				System.err.println("Connection refused from: " + workerAddress + ". Trying again");
				e.printStackTrace();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ex) {
					ex.printStackTrace();
				}
			}
		}
	}


}
