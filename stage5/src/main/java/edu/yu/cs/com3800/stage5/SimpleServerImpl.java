package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.SimpleServer;

import java.io.*;
import java.net.*;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.logging.*;

public class SimpleServerImpl implements SimpleServer {
	int port;
	HttpServer server;
	private final static Logger SERVER_LOGGER = Logger.getLogger("ServerLogger");

	public SimpleServerImpl(int port) throws IOException {
		this.port = port;
		FileHandler fh;

		try {
			long time = new GregorianCalendar().getTimeInMillis();

			// This block configure the logger with handler and formatter
			fh = new FileHandler("./Log" + time  +".log"); // <<<<<<<<<<<<<<<<<<<<FIX
			SERVER_LOGGER.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);
			SERVER_LOGGER.setUseParentHandlers(false);

		} catch (SecurityException | IOException e) {
			e.printStackTrace();
		}


	}
	@Override
	public void start() {
		try {
			server = HttpServer.create(new InetSocketAddress(this.port), 0);
		} catch (IOException e) {
			e.printStackTrace();
		}
		server.createContext("/compileandrun", new POSTHandler());
		server.setExecutor(null);
		server.start();
		SERVER_LOGGER.info("Server Started Successfully");

	}
	class POSTHandler implements HttpHandler {
		public void handle(HttpExchange t) throws IOException {
			if(!t.getRequestMethod().equals("POST")){
				t.sendResponseHeaders(405, -1);
				OutputStream os = t.getResponseBody();
				os.close();
				//LOG HERE BUT WAIT FOR PIAZZA
				SERVER_LOGGER.info("(405) Server received a non-POST");
				t.close();
				return;
			}
			if(!t.getRequestHeaders().get("Content-Type").get(0).equals("text/x-java-source")){ //double check
				SERVER_LOGGER.info("(400) Content Type received was not text/x-java-source");
				t.sendResponseHeaders(400, -1);
				OutputStream os = t.getResponseBody();
				os.close();
				t.close();
				return;
			}

			/*StringBuilder srcCodeSB = new StringBuilder();
			for(String line : t.getRequestHeaders().get("code")){
				srcCodeSB.append(line + "\n");
			}
			String srcCode = srcCodeSB.toString();
			InputStream is = new ByteArrayInputStream(srcCode.getBytes());*/
			InputStream requestBodyInputStream = t.getRequestBody();
			//
			byte[] buff = new byte[8000];
			int bytesRead = 0;
			ByteArrayOutputStream bao = new ByteArrayOutputStream();
			while((bytesRead = requestBodyInputStream.read(buff)) != -1) {
				bao.write(buff, 0, bytesRead);
			}
			byte[] data = bao.toByteArray();
			ByteArrayInputStream bin = new ByteArrayInputStream(data);
			//
			/*int bufferSize = 1024;
			char[] buffer = new char[bufferSize];
			StringBuilder out = new StringBuilder();
			Reader in = new InputStreamReader(requestBodyInputStream, "UTF-8");
			for (int numRead; (numRead = in.read(buffer, 0, buffer.length)) > 0; ) {
				out.append(buffer, 0, numRead);
			}
			System.out.println(out);*/
			//
			JavaRunner jr = new JavaRunner();
			String output = null;
			try {
				output = jr.compileAndRun(bin);
			} catch (Exception e) {
				ByteArrayOutputStream stackTraceBaos = new ByteArrayOutputStream();
				PrintStream ps = new PrintStream(stackTraceBaos);
				e.printStackTrace(ps);
				String stackTrace = stackTraceBaos.toString();
				String exceptionResponse = e.getMessage() + "\n" + stackTrace;
				t.sendResponseHeaders(400, exceptionResponse.length());
				OutputStream os = t.getResponseBody();
				os.write(exceptionResponse.getBytes());
				os.close();
				ps.close();
				stackTraceBaos.close();
				t.close();
				SERVER_LOGGER.info("(400) Java code threw an exception:\n" + exceptionResponse + "\n");
				return;
			}
			requestBodyInputStream.close();
			bin.close();
			bao.close();
			t.sendResponseHeaders(200, output.length());
			OutputStream os = t.getResponseBody();
			os.write(output.getBytes());
			os.close();
			SERVER_LOGGER.info("(200) Java code ran properly. Output: " + output);
			t.close();
			return;

		}
	}

	@Override
	public void stop() {
		SERVER_LOGGER.info("Server stopped");

		server.stop(0);
	}

	public static void main(String[] args)
	{
		int port = 9000;
		if(args.length >0)
		{
			port = Integer.parseInt(args[0]);
		}
		SimpleServer myserver = null;
		try
		{
			myserver = new SimpleServerImpl(port);
			myserver.start();
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
			myserver.stop();

		}
	}
}
