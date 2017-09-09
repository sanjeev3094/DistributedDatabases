package com.adb.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseHandler {

	public static final String DATABASE_DRIVER = "com.mysql.jdbc.Driver";

	public static Connection getConnection(String url, String username,
			String password) {
		Connection conn = null;
		try 
		{
			Class.forName(DATABASE_DRIVER);
			conn = DriverManager.getConnection(url, username, password);
		}
		catch (ClassNotFoundException cnfe) 
		{
			System.out.println(" Error : Could not find the JDBC driver  !!!");
		}
		catch (SQLException sqle) 
		{
			System.out.println(" Error : Could not connect "
					+ "to the data base ( registration problem ) !!!");
		}
		return conn;
	}
	
	public static void closeConnection(Connection conn)
	{
		if( conn != null)
		{
			try 
			{
				conn.close();
			} 
			catch (SQLException e)
			{
					System.out.println(" Error : cannot close the connection  !!!  ");
			}
		}
	}
}
