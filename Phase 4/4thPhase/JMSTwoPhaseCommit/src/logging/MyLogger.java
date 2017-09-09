package logging;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class MyLogger {
	static private FileHandler fileTxt1;
	static private SimpleFormatter formatterTxt1;

	static private FileHandler fileHTML1;
	static private Formatter formatterHTML1;

	static private FileHandler fileTxt2;
	static private SimpleFormatter formatterTxt2;

	static private FileHandler fileHTML2;
	static private Formatter formatterHTML2;

	static private FileHandler fileTxt3;
	static private SimpleFormatter formatterTxt3;

	static private FileHandler fileHTML3;
	static private Formatter formatterHTML3;
	static public void setup1() throws IOException 
	{
		// Create Logger
		Logger logger = Logger.getLogger("");
		logger.setLevel(Level.INFO);
		fileTxt1 = new FileHandler("Logging1.txt");
		fileHTML1 = new FileHandler("Logging1.html");

		// Create txt Formatter
		formatterTxt1 = new SimpleFormatter();
		fileTxt1.setFormatter(formatterTxt1);
		logger.addHandler(fileTxt1);

		// Create HTML Formatter
		formatterHTML1 = new MyHtmlFormatter();
		fileHTML1.setFormatter(formatterHTML1);
		logger.addHandler(fileHTML1);
	}
	static public void setup2() throws IOException 
	{
		// Create Logger
		Logger logger = Logger.getLogger("");
		logger.setLevel(Level.INFO);
		fileTxt2 = new FileHandler("Logging2.txt");
		fileHTML2 = new FileHandler("Logging2.html");
	
		// Create txt Formatter
		formatterTxt2 = new SimpleFormatter();
		fileTxt2.setFormatter(formatterTxt2);
		logger.addHandler(fileTxt2);
	
		// Create HTML Formatter
		formatterHTML2 = new MyHtmlFormatter();
		fileHTML2.setFormatter(formatterHTML2);
		logger.addHandler(fileHTML2);
	}
	static public void setup3() throws IOException 
	{
		// Create Logger
		Logger logger = Logger.getLogger("");
		logger.setLevel(Level.INFO);
		fileTxt3 = new FileHandler("Logging3.txt");
		fileHTML3 = new FileHandler("Logging3.html");
	
		// Create txt Formatter
		formatterTxt3 = new SimpleFormatter();
		fileTxt3.setFormatter(formatterTxt3);
		logger.addHandler(fileTxt3);
	
		// Create HTML Formatter
		formatterHTML3 = new MyHtmlFormatter();
		fileHTML3.setFormatter(formatterHTML3);
		logger.addHandler(fileHTML3);
	}
}
