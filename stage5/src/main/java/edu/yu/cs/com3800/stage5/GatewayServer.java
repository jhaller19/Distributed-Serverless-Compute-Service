package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Logger;

public class GatewayServer implements LoggingServer {
	InetSocketAddress address;
	int port;
	HttpServer httpServer;
	GatewayPeerServerImpl gatewayPeerServer;
	Logger logger;
	HttpServer httpServerForScript; //8081
	HttpServer httpServerIsDead; //8083
	HttpServer httpServerTest;//8079
	static String time = new SimpleDateFormat("yyyy-MM-dd-kk_mm").format(Calendar.getInstance().getTime());


	public GatewayServer(int port , GatewayPeerServerImpl gatewayPeerServer){
		this.port = port;
		this.address = new InetSocketAddress("localhost", this.port);
		this.gatewayPeerServer = gatewayPeerServer;
		logger = initializeLogging("GatewayServer-http-port-" + port);
	}
	public void start(){
		/**STEP 2: Listen for HTTP connection from client*/
		try {
			httpServer = HttpServer.create(new InetSocketAddress(this.port), 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		httpServer.createContext("/compileandrun", new ClientHandler());
		//TODO: Threadpool
		httpServer.setExecutor(Executors.newFixedThreadPool(2*Runtime.getRuntime().availableProcessors()));
		httpServer.start();
		//start up GatewayServerImpl
		//gatewayPeerServer.start();
		logger.info("Started successfully and waiting for client connections...");
		startHttpService();
		startIsDeadService();
		startTestService();
	}
	private void startIsDeadService(){
		try {
			httpServerIsDead = HttpServer.create(new InetSocketAddress(this.port+3), 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		httpServerIsDead.createContext("/isDead", new IsDeadHandler());
		//TODO: Threadpool
		httpServerIsDead.setExecutor(Executors.newFixedThreadPool(2*Runtime.getRuntime().availableProcessors()));
		httpServerIsDead.start();
		//start up GatewayServerImpl
		//gatewayPeerServer.start();
		logger.finest("Http is dead service successfully on port " + (this.port+3));
	}
	private void startHttpService(){
		try {
			httpServerForScript = HttpServer.create(new InetSocketAddress(this.port+1), 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		httpServerForScript.createContext("/getNodes", new ScriptClientHandler());
		//TODO: Threadpool
		httpServerForScript.setExecutor(Executors.newFixedThreadPool(2*Runtime.getRuntime().availableProcessors()));
		httpServerForScript.start();
		//start up GatewayServerImpl
		//gatewayPeerServer.start();
		logger.finest("Http service successfully on port " + (this.port+1));
	}
	private void startTestService(){
		try {
			httpServerTest = HttpServer.create(new InetSocketAddress(this.port-1), 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		httpServerTest.createContext("/time", new TimeHandler());
		httpServerTest.setExecutor(Executors.newFixedThreadPool(2*Runtime.getRuntime().availableProcessors()));
		httpServerTest.start();

	}

	public void shutdown(){
		logger.severe("Shutting down");
		for(Handler h : logger.getHandlers()){
			h.close();
		}
		httpServer.stop(0);
		httpServerForScript.stop(0);
	}

	AtomicInteger clientHandlerCount = new AtomicInteger(1);
	class ClientHandler implements HttpHandler {
		ThreadLocal<Logger> threadLocalLogger = new ThreadLocal<>();
		/**
		 * synchronously communicate with the master/leader over TCP to submit the client request and get a
		 * response. Be careful to not have any instance variables in your HttpHandler – its methods must be thread safe!
		 * Only use local variables in your methods
		 */
		public void handle(HttpExchange t) throws IOException {
			if(threadLocalLogger.get() == null){
				threadLocalLogger.set(initializeLogging("ClientHandler-" + clientHandlerCount.getAndIncrement()));
			}
			Logger clientHandlerLogger = threadLocalLogger.get();
			if(!isValidRequest(t , clientHandlerLogger)){
				return;
			}
			boolean success = false;
			while(!success) {
				while(gatewayPeerServer.getCurrentLeader() == null){}
				clientHandlerLogger.info(Thread.currentThread().getName() + " Handling http request from client...");
				InetSocketAddress currentLeader = gatewayPeerServer.getPeerByID(gatewayPeerServer.getCurrentLeader().getProposedLeaderID());
				clientHandlerLogger.info("Sending work to leader: " + currentLeader);

				String hostNameOfLeader = currentLeader.getHostName();
				int tcpPortNumberOfLeader = currentLeader.getPort() + 2; //not sure
				try (
						/**STEP 3: Establish TCP connection with leader*/
						Socket socket = new Socket(hostNameOfLeader, tcpPortNumberOfLeader);
						InputStream is = socket.getInputStream();
						OutputStream out = socket.getOutputStream();
				) {
					clientHandlerLogger.fine("Connection made with leader");
					//Send message to leader over the connection
					byte[] request = t.getRequestBody().readAllBytes();
					Message httpRequestMessageToSendToLeader = new Message(Message.MessageType.WORK, request, address.getHostName(), port, hostNameOfLeader, tcpPortNumberOfLeader);
					clientHandlerLogger.finer("Sending WORK message to leader containing the Http request: \n" + httpRequestMessageToSendToLeader.toString());
					out.write(httpRequestMessageToSendToLeader.getNetworkPayload());

					/**STEP 10: Receive finished work from leader*/
					//Synchronously wait for response from leader
					clientHandlerLogger.fine("Waiting for COMPLETED_WORK from leader...");
					byte[] responseNetworkPayload = Util.readAllBytesFromNetwork(is);
					Message responseMessage = new Message(responseNetworkPayload);
					clientHandlerLogger.finer("Received COMPLETED_WORK message from leader\n" + responseMessage.toString());
					//Close connection
					socket.close(); //TODO: should i be closing here?
					if(gatewayPeerServer.isPeerDead(currentLeader)){
						continue;
					}
					/**STEP 11: Send finished work back to client*/
					//Send response back to client
					//TODO: ErrorOccurred!
					if (responseMessage.getErrorOccurred()) {
						t.sendResponseHeaders(400, responseMessage.getMessageContents().length);
					} else {
						t.sendResponseHeaders(200, responseMessage.getMessageContents().length);
					}
					OutputStream os = t.getResponseBody();
					os.write(responseMessage.getMessageContents());
					os.close();
					t.close();
					clientHandlerLogger.info("Sent response back to client!");
					success = true;
				} catch (Exception e) {
					clientHandlerLogger.severe("Connection refused b/c Leader is not ready for work");
					try {
						Thread.sleep(5000);
					} catch (InterruptedException ex) {
						ex.printStackTrace();
					}
				}
			}
		}
		private boolean isValidRequest(HttpExchange t, Logger clientHandlerLogger) throws IOException {
			if(!t.getRequestMethod().equals("POST")){
				t.sendResponseHeaders(405, 0);
				OutputStream os = t.getResponseBody();
				os.close();
				//LOG HERE BUT WAIT FOR PIAZZA
				clientHandlerLogger.info("(405) Server received a non-POST");
				t.close();
				return false;
			}
			if(!t.getRequestHeaders().get("Content-Type").get(0).equals("text/x-java-source")){ //double check
				clientHandlerLogger.info("(400) Content Type received was not text/x-java-source");
				t.sendResponseHeaders(400, 0);
				OutputStream os = t.getResponseBody();
				os.close();
				t.close();
				return false;
			}
			return true;
		}
	}

	class ScriptClientHandler implements HttpHandler {

		@Override
		public void handle(HttpExchange exchange) throws IOException {
			logger.fine("Starting to handle request for node list");
			while(gatewayPeerServer.getCurrentLeader() == null){
			}
			StringBuilder sb = new StringBuilder();
			sb.append("List of Nodes: [");
			sb.append(gatewayPeerServer.currentLeader.getProposedLeaderID()).append(": Leader]");
			for(long i : gatewayPeerServer.peerIDtoAddress.keySet()){
				if(i == gatewayPeerServer.id)continue;
				if(i != gatewayPeerServer.currentLeader.getProposedLeaderID()){
					sb.append("[");
					sb.append(i).append(": Follower]");
				}
			}
			String response = sb.toString();
			exchange.sendResponseHeaders(200, response.getBytes().length);
			exchange.getResponseBody().write(response.getBytes());
			logger.fine("Finished handling request for node list");
			//System.out.println(sb.toString());
			//System.out.println("DONE HANDLING");
		}
	}
	class IsDeadHandler implements HttpHandler{
		@Override
		public void handle(HttpExchange exchange) throws IOException {
			String deadServer = new String(exchange.getRequestBody().readAllBytes());
			int id = Integer.parseInt(deadServer);
			while(!gatewayPeerServer.isPeerDead(id)){
			}
			String response = "Gateway noticed " + id + " is dead\n";
			exchange.sendResponseHeaders(200, response.getBytes().length);
			exchange.getResponseBody().write(response.getBytes());
		}
	}
	class TimeHandler implements HttpHandler{
		@Override
		public void handle(HttpExchange exchange) throws IOException {
			exchange.sendResponseHeaders(200, time.getBytes().length);
			exchange.getResponseBody().write(time.getBytes());
		}
	}

	public static void main(String[] args) {
		ConcurrentHashMap<Long,InetSocketAddress> map = new ConcurrentHashMap<>();
		int port = 8010;
		int myPort = 8082;
		long myId = 8;//Integer.parseInt(args[0]);
		for (long i = 1; i <= 7; i++) {
			map.put(i, new InetSocketAddress("localhost", port));
			port += 10;
		}
		map.put(8L, new InetSocketAddress("localhost" , myPort));
		GatewayPeerServerImpl gatewayPeerServer = new GatewayPeerServerImpl(myPort, 0, myId, map, 1, 8082);
		GatewayServer gatewayServer = new GatewayServer(8080, gatewayPeerServer);
		gatewayPeerServer.start();
		gatewayServer.start();
	}
}
