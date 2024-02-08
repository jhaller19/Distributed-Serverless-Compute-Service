package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ScriptTest {
	Process leaderProcess;
	List<Process> followers = new ArrayList<>();
	Process gatewayProcess;
	int gatewayServerAddress;
	Client c = new Client();
	String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";

	@After
	public void after() throws InterruptedException {
		Thread.sleep(2000);
	}
	@Before
	public void before(){
		System.out.println("***********************************************");
	}
	@Test
	public void killLeaderAfterSendingRequests() throws IOException, InterruptedException {
		gatewayProcess = GatewayRunner.exec(5);
		leaderProcess = LeaderRunner.exec();
		for(int i = 3; i >= 1 ; i--){
			Process follower = FollowerRunner.exec(i);
			followers.add(follower);
		}
		c.getNodes();
		Thread.sleep(5000);
		System.out.println("Sending request and then killing leader");
		c.sendRequests(1, validClass);
		leaderProcess.destroyForcibly().waitFor();
		for(HttpResponse<String> h : c.getResponses()){
			System.out.println(h.body());
		}
		shutdownAll();
	}
	@Test
	public void killLeaderBeforeSendingRequests() throws IOException, InterruptedException {
		gatewayProcess = GatewayRunner.exec(5);
		leaderProcess = LeaderRunner.exec();
		for(int i = 3; i >= 1 ; i--){
			Process follower = FollowerRunner.exec(i);
			followers.add(follower);
		}
		c.getNodes();
		Thread.sleep(5000);
		System.out.println("Killing leader and then sending request");
		leaderProcess.destroyForcibly().waitFor();
		Thread.sleep(3000);
		c.sendRequests(1, validClass);
		for(HttpResponse<String> h : c.getResponses()){
			System.out.println(h.body());
		}
		shutdownAll();
	}
	@Test
	public void killFollowerBeforeSendingRequest() throws IOException, InterruptedException {
		gatewayProcess = GatewayRunner.exec(5);
		leaderProcess = LeaderRunner.exec();
		for(int i = 3; i >= 1 ; i--){
			Process follower = FollowerRunner.exec(i);
			followers.add(follower);
		}
		c.getNodes();
		Thread.sleep(7000);
		followers.get(followers.size()-1).destroyForcibly().waitFor();
		Thread.sleep(2000);
		c.sendRequests(1, validClass);
		for(HttpResponse<String> h : c.getResponses()){
			System.out.println(h.body());
		}
		shutdownAll();
	}
	@Test
	public void killFollowerAfterSendingRequest() throws IOException, InterruptedException {
		gatewayProcess = GatewayRunner.exec(5);
		leaderProcess = LeaderRunner.exec();
		for(int i = 3; i >= 1 ; i--){
			Process follower = FollowerRunner.exec(i);
			followers.add(follower);
		}
		c.getNodes();
		Thread.sleep(10000);
		c.sendRequests(1, validClass);
		followers.get(followers.size()-1).destroyForcibly().waitFor();
		for(HttpResponse<String> h : c.getResponses()){
			System.out.println(h.body());
		}
		shutdownAll();
	}
	private void shutdownAll() throws InterruptedException {
		leaderProcess.destroyForcibly().waitFor();
		for(Process p : followers){
			p.destroyForcibly().waitFor();
		}
		gatewayProcess.destroyForcibly().waitFor();
	}

	class Client{
		HttpClient httpClient;
		List<CompletableFuture<HttpResponse<String>>> responses;
		Client(){
			httpClient = HttpClient.newBuilder()
					.version(HttpClient.Version.HTTP_2)
					.build();
			responses = new ArrayList<>();
		}
		void getNodes() throws IOException, InterruptedException {
			HttpRequest request = HttpRequest.newBuilder()
					.GET()
					.uri(URI.create("http://" + "localhost" + ":" + "8051" + "/getNodes"))
					.setHeader("User-Agent", "Java 11 HttpClient Bot")
					.build();

			HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
			//System.out.println(response.body());
		}

		void sendRequests(int nRequests, String srcCode){
			for(int i = 0; i < nRequests; i++){
				String codeToSend = srcCode.replace("world!", "world! from code version " + i);
				HttpRequest httpRequest = HttpRequest.newBuilder()
						.POST(HttpRequest.BodyPublishers.ofString(codeToSend))
						.uri(URI.create("http://" + "localhost" + ":" + "8050" + "/compileandrun"))
						.setHeader("Content-Type", "text/x-java-source")
						.build();
				this.responses.add(httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString()));
			}
		}
		List<HttpResponse<String>> getResponses(){
			List<HttpResponse<String>> allResponses = new ArrayList<>();
			for(CompletableFuture<HttpResponse<String>> futureResponse : this.responses){
				HttpResponse<String> response = null;
				try {
					response = futureResponse.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
				allResponses.add(response);
			}
			return allResponses;
		}
	}


}
