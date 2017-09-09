package com.adb.Executor.loadDBProfile;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

import com.adb.Executor.SDD1.DatabaseProfile;
import com.adb.Executor.SDD1.SelectivityFactorAndSizeAttr;

public class LoadDBProfile
{
	public static DatabaseProfile dbProfileALL = new DatabaseProfile();

	static
	{
		// deserialize the DBProfile Map
		try
		{
			FileInputStream fis = new FileInputStream("CompleteDBProfile.ser");
			ObjectInputStream ois = new ObjectInputStream(fis);
			dbProfileALL = (DatabaseProfile) ois.readObject();
			ois.close();
		}
		catch (Exception e)
		{
			System.out.println("Exception during deserialization: " + e);
		}
	}

	public void printMap()
	{
		HashMap<String, SelectivityFactorAndSizeAttr> atrDMap = dbProfileALL.getAttrDetailMap();
		System.out.println("SF  :  Size");
		for (Map.Entry<String, SelectivityFactorAndSizeAttr> entry : atrDMap.entrySet())
		{
			System.out.println(entry.getKey() + " " + entry.getValue().getSelectivityFactor() + " " + entry.getValue().getSizeAttr());
		}
	}

	public void printRelMap()
	{
		HashMap<String, Double> relationSizeMap = dbProfileALL.getRelationSizeMap();
		System.out.println("Relation   :   Size");
		for (Map.Entry<String, Double> entry : relationSizeMap.entrySet())
		{
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
	}
}
