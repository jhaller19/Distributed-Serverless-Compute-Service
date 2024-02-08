package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class ClientImpl implements Client {

	String hostName;
	int hostPort;
	HttpClient httpClient;
	Response responseObj;


	public ClientImpl(String hostName, int hostPort) throws MalformedURLException {
		this.hostName = hostName;
		this.hostPort = hostPort;
		httpClient = HttpClient.newBuilder()
				.version(HttpClient.Version.HTTP_2)
				.build();
	}

	@Override
	public void sendCompileAndRunRequest(String src) throws IOException {
		if(src == null){
			throw new IllegalArgumentException("src cant be null");
		}
		HttpRequest httpRequest = HttpRequest.newBuilder()
				.POST(HttpRequest.BodyPublishers.ofString(src))
				.uri(URI.create("http://" + hostName + ":" + hostPort + "/compileandrun"))
				.setHeader("Content-Type", "text/x-java-source")
				.build();



		/*HttpRequest.Builder builder = HttpRequest.newBuilder()
				.GET()
				.uri(URI.create("http://" + hostName + ":" + hostPort + "/compileandrun"))
				.setHeader("Content-Type", "text/x-java-source");
		String[] codeSplitByNewLine = src.split("\n");
		for(String line : codeSplitByNewLine){
			builder.header("code" , line);
		}
		HttpRequest request = builder.build();*/
		/*HttpRequest request = HttpRequest.newBuilder()
				.GET()
				.uri(URI.create("http://" + hostName + ":" + hostPort + "/compileandrun"))
				.setHeader("Content-Type", "text/x-java-source")
				.setHeader("code" , src)
				.header("code" , "heelp")
				.build();*/




		HttpResponse<String> response = null;
		try {
			response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.responseObj = new Response(response.statusCode() , response.body());

	}

	@Override
	public Response getResponse() throws IOException {
		return this.responseObj;
	}

	/*public static void main(String[] args) throws IOException {
		Client c = new ClientImpl("localhost" , 9000);
		c.sendCompileAndRunRequest("a");
		System.out.println(c.getResponse());
	}*/


}
