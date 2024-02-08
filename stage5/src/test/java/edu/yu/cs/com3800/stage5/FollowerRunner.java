package edu.yu.cs.com3800.stage5;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class FollowerRunner {
	public static long gatewayServerID = 0;
	public static int[] ports = {8010, 8020, 8030, 8040, 8052};
	public static int leaderPort = 8040;
	public static long leaderID = 4L;
	long myId;

	public static void main(String[] args) throws IOException {
		int id = Integer.parseInt(args[0]);
		FollowerRunner runner = new FollowerRunner(id);
		runner.createLeader();
	}

	private FollowerRunner(int id) {
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
		ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(ports[(int)myId - 1], 0, myId, peerIDtoAddress, 1, 8052);
		server.start();
	}

	public static Process exec(int id) throws IOException {
		String javaHome = System.getProperty("java.home");
		String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
		String classpath = System.getProperty("java.class.path");
		String className = FollowerRunner.class.getName();
		List<String> command = new LinkedList<>();
		command.add(javaBin);
		command.add("-cp");
		command.add(classpath);
		command.add(className);
		command.add(String.valueOf(id));

		ProcessBuilder builder = new ProcessBuilder(command);
		Process follower = builder.inheritIO().start();
		pause(500);
		while(!follower.isAlive()){
			pause(500);
		}
		return follower;
	}

	private static void pause(long time){
		try {
			Thread.sleep(1000);
		}
		catch (InterruptedException e) {
		}
	}


}
