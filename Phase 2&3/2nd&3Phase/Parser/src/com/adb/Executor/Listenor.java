package com.adb.Executor;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.adb.Executor.SDD1.FragmentStatistics;
import com.adb.utils.DatabaseHandler;

public class Listenor
{
	public static final String URL = "jdbc:mysql://localhost:3306/college";
	public static final String USER_NAME = "root" ;
	public static final String PASSWORD = "openstack" ;
	public static final String TABLE_SCHEMA = "college";
	
	public static void loadStatistics(ArrayList<String> fragNameList,HashMap<String,FragmentStatistics> fragmentStatMap,
			 HashMap<String,Integer> VRAttrMap)
	{
		Connection conn = DatabaseHandler.getConnection(URL, USER_NAME, PASSWORD);
		
		try
		{	 
			for(String fragName:fragNameList)
			{
				PreparedStatement ps = conn.prepareStatement("select * from " + fragName);
				ResultSet rs = ps.executeQuery();

				HashMap<String,ArrayList<String>> attrNameDistinctValMap = new HashMap<String, ArrayList<String>>();
				ResultSetMetaData rsmd = rs.getMetaData();

				int columnsNumber = rsmd.getColumnCount();
				ArrayList<String> attrNameList = new ArrayList<String>();

				Integer tupleSize = 0;
				
				for(int i=1;i<=columnsNumber;++i)
				{
					String attrName = rsmd.getColumnName(i);
					String attrType = rsmd.getColumnTypeName(i);
					attrNameList.add(attrName);
					
					Integer colSize = rsmd.getColumnDisplaySize(i);
					if(attrType.compareTo("date")==0 || attrType.compareTo("time")==0)
						colSize = 3;
					if(attrType.compareTo("datetime")==0)
						colSize = 8;
					if(attrType.compareTo("float")==0 || attrType.contains("int"))
						colSize = 4;
					
					tupleSize += colSize;
					
//					System.out.println(fragName +" "+attrName + "attrSize : " + rsmd.getColumnDisplaySize(i));
//					System.out.println(attrName);
//					System.out.println("Name of ["+i+"] Column data type is =" + rsmd.getColumnTypeName(i));
				}
				
				System.out.println(fragName + "  " + tupleSize);
				
				Integer noOfTuples = 0;

				while (rs.next())
				{
					noOfTuples++;
					System.out.println(rs.getString(1));
					for(String attrName:attrNameList)
					{
						String val = rs.getString(attrName);
						if(val == null)
							continue;
						if(attrNameDistinctValMap.containsKey(attrName))
						{
							ArrayList<String> attrValList = attrNameDistinctValMap.get(attrName);
							if(!attrValList.contains(val))
							{
								attrValList.add(val);
								attrNameDistinctValMap.put(attrName, attrValList);
							}
						}
						else
						{
							ArrayList<String> attrValList = new ArrayList<String>();
							attrValList.add(val);
							attrNameDistinctValMap.put(attrName, attrValList);
						}
					}
				}
				
				for(String attrName:attrNameList)
				{
					String key = "("+ fragName +","+attrName+")";
					Integer val = 0;
					if(attrNameDistinctValMap.containsKey(attrName))
					{
						val = attrNameDistinctValMap.get(attrName).size();
					}
					VRAttrMap.put(key, val);					
				}
				
				FragmentStatistics fragStatObj = new FragmentStatistics();
				fragStatObj.setName(fragName);
				fragStatObj.setAttrList(attrNameList);
				fragStatObj.setNoOfTuples(noOfTuples);
				fragmentStatMap.put(fragName, fragStatObj);		
			}
		}
		catch(NullPointerException npe)
		{
			System.out.println(" Error : Null pointer exception !!! ");
		}
		catch(SQLException sqle)
		{ 
			System.out.println(" Error : cannot execute the query !!! ");
		}
		finally
		{
			DatabaseHandler.closeConnection(conn);
		}
	}
	
	public static void main (String [] args) throws IOException 
	{
		ServerSocket myNewSocket = null ;
		boolean flag = true;

		myNewSocket =  new ServerSocket (5000);

		System.out.println("*********SERVER STARTED ********");

		while(flag)
		{
			new MulticlientThread(myNewSocket.accept()).start();
		}

		myNewSocket.close();
	}
}

class MulticlientThread extends Thread
{

	private Socket newsocket = null;
	
	void printVAttrMap(HashMap<String,Integer> VRAttrMap)
	{
		for(Map.Entry<String,Integer> entry:VRAttrMap.entrySet())
		{
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
	}

	public MulticlientThread(Socket socket) 
	{
		super("MulticlientThread");
		this.newsocket = socket;
	}

	@SuppressWarnings("unchecked")
	public void run()
	{
		try{

//			BufferedReader in = new BufferedReader (new InputStreamReader (newsocket.getInputStream()));
//			PrintWriter out = new PrintWriter(newsocket.getOutputStream(),true);
			InputStream inputStream = newsocket.getInputStream(); 
			ObjectInput objectInput = new ObjectInputStream(inputStream);
			Object obj = objectInput.readObject();
			
			HashMap<String,FragmentStatistics> fragmentStatMap = new HashMap<String, FragmentStatistics>();
			HashMap<String,Integer> VRAttrMap = new HashMap<String, Integer>();
			
			ArrayList<String> fragnameList = (ArrayList<String>) obj; 
			Listenor.loadStatistics(fragnameList, fragmentStatMap,VRAttrMap);
			
			Statistics stSet = new Statistics();
			stSet.setFragmentStatMap(fragmentStatMap);
			stSet.setVRAttrMap(VRAttrMap);
			
			OutputStream outputStream = newsocket.getOutputStream();
			ObjectOutput objectOutput = new ObjectOutputStream(outputStream);
			objectOutput.writeObject(stSet);
			
			printVAttrMap(VRAttrMap);
		}
		catch(Exception e)
		{
		}
	}
}

