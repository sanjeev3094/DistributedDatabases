package com.adb.Executor;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ServicesUtitlity
{
	public static Integer PORT_NO = 5000;
	
	void printVAttrMap(HashMap<String,Integer> VRAttrMap)
	{
		for(Map.Entry<String,Integer> entry:VRAttrMap.entrySet())
		{
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
	}
	
	public void findStatisticsOfFragments(ArrayList<String> fragments)
	{
		String server = "localhost";
		try
		{
			Socket clientsocket = new Socket(server, PORT_NO);
			OutputStream outputStream = clientsocket.getOutputStream();
			ObjectOutput objectOutput = new ObjectOutputStream(outputStream);
			objectOutput.writeObject(fragments);
			
//			BufferedReader inputReader = new BufferedReader (new InputStreamReader (clientsocket.getInputStream()));
			
			InputStream inputStream = clientsocket.getInputStream(); 
			ObjectInput objectInput = new ObjectInputStream(inputStream);
			Object replyObject = objectInput.readObject();
			
			Statistics stSet = (Statistics) replyObject;
			printVAttrMap(stSet.getVRAttrMap());
			
			outputStream.close();
			clientsocket.close();
		}
		catch (UnknownHostException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
		}
		
	}
}
