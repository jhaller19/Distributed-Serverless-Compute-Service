package edu.yu.cs.com3800;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.logging.*;

public interface LoggingServer {

	String strDate = new SimpleDateFormat("yyyy-MM-dd-kk_mm").format(Calendar.getInstance().getTime());
	default Logger initializeLogging(String name){
		//System.out.println(strDate);
		Logger logger = Logger.getLogger(name);
		try {
			//String strDate = new SimpleDateFormat("yyyy-MM-dd-kk_mm").format(Calendar.getInstance().getTime());
			String[] split = name.split("-");
			String dirLocation = "." + File.separator + "logs" + File.separator + strDate + File.separator + split[0] + "Logs";
			File file = new File(dirLocation);
			file.mkdirs();
			String path = dirLocation + File.separator + name + ".log";
			FileHandler fileHandler = new FileHandler(path, false);
			logger.addHandler(fileHandler);
			fileHandler.setFormatter(new SimpleFormatter());
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.setLevel(Level.ALL);
		logger.setUseParentHandlers(false);

		/*Set console handler*/
		/*ConsoleHandler handler = new ConsoleHandler();
		handler.setFormatter(new SimpleFormatter());
		handler.setLevel(Level.INFO);
		logger.addHandler(handler);*/

		//
		return logger;
		/*File file = new File(System.getProperty("user.dir") + File.separator + "logs" + File.separator + name.split("-")[0]);
		if(!file.exists()){
			file.mkdirs();
		}
		long time = new GregorianCalendar().getTimeInMillis();
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH.mm.ss");
		LocalDateTime now = LocalDateTime.now();
		String date = dtf.format(now);
		Logger logger = Logger.getLogger(name);
		FileHandler fh = null;
		try {
			fh = new FileHandler(file.getPath() + File.separator + name + "--" + time + ".log");

		} catch (IOException e) {
			e.printStackTrace();
		}
		assert fh != null;
		logger.addHandler(fh);
		SimpleFormatter formatter = new SimpleFormatter();
		fh.setFormatter(formatter);
		logger.setUseParentHandlers(false);
		logger.setLevel(Level.ALL);
		return logger;*/
	}
	


}
