package edu.yu.cs.com3800.stage5;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LeaderRunner {
    public static long gatewayServerID = 0;
    public static int[] ports = {8010, 8020, 8030, 8040, 8052};
    public static int leaderPort = 8040;
    public static long leaderID = 4L;

    public static void main(String[] args) throws IOException {
        LeaderRunner runner = new LeaderRunner();
        runner.createLeader();
    }

    private LeaderRunner() {

    }

    private void createLeader() throws IOException {
        //create leader
        //create IDs and addresses
        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(5);
        for (int i = 1; i < this.ports.length + 1; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", this.ports[i - 1]));
        }
        peerIDtoAddress.remove(leaderID);
        ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(leaderPort, 0, leaderID, peerIDtoAddress, 1, 8052);
        server.start();
    }

    public static Process exec() throws IOException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = LeaderRunner.class.getName();
        List<String> command = new LinkedList<>();
        command.add(javaBin);
        command.add("-cp");
        command.add(classpath);
        command.add(className);

        ProcessBuilder builder = new ProcessBuilder(command);
        Process leader = builder.inheritIO().start();
        pause(500);
        while(!leader.isAlive()){
            System.out.println("h");
            pause(500);
        }
        return leader;
    }

    private static void pause(long time){
        try {
            Thread.sleep(1000);
        }
        catch (InterruptedException e) {
        }
    }


}
