package edu.yu.cs.com3800.stage5;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class GatewayRunner {
	public static long gatewayServerID = 0;
	public static int[] ports = {8010, 8020, 8030, 8040, 8052};
	long myId;

	public static void main(String[] args) throws IOException {
		//args = new String[]{"8"};
		int id = Integer.parseInt(args[0]);
		GatewayRunner runner = new GatewayRunner(id);
		runner.createLeader();
	}

	private GatewayRunner(int id) {
		myId = id;
	}

	private void createLeader() throws IOException {
		//create follower
		//create IDs and addresses
		ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(5);
		for (int i = 1; i < this.ports.length + 1; i++) {
			peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", this.ports[i - 1]));
		}
		peerIDtoAddress.remove(myId);
		GatewayPeerServerImpl server = new GatewayPeerServerImpl(8052, 0, myId, peerIDtoAddress, 1, 8052);
		GatewayServer gatewayServer = new GatewayServer(8050, server);
		server.start();
		gatewayServer.start();
	}

	public static Process exec(int id) throws IOException {
		String javaHome = System.getProperty("java.home");
		String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
		String classpath = System.getProperty("java.class.path");
		String className = GatewayRunner.class.getName();
		List<String> command = new LinkedList<>();
		command.add(javaBin);
		command.add("-cp");
		command.add(classpath);
		command.add(className);
		command.add(String.valueOf(id));

		ProcessBuilder builder = new ProcessBuilder(command);
		Process gateway = builder.inheritIO().start();
		pause(500);
		while(!gateway.isAlive()){
			pause(500);
		}
		return gateway;
	}

	private static void pause(long time){
		try {
			Thread.sleep(1000);
		}
		catch (InterruptedException e) {
		}
	}


}
