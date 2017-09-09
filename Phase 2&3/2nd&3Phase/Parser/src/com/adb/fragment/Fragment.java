package com.adb.fragment;
import java.util.ArrayList;

import com.adb.general.Site;
import com.adb.general.Table;

public class Fragment
{
	String name;
	String type;
	Table table;
	
	ArrayList<Site> site;
	
	public Fragment()
	{
		super();
	}

	public Fragment(String name, String type, ArrayList<Site> site, Table table)
	{
		super();
		this.name = name;
		this.type = type;
		this.site = site;
		this.table = table;
	}
	
	public String getName()
	{
		return name;
	}
	public void setName(String name)
	{
		this.name = name;
	}
	public String getType()
	{
		return type;
	}
	public void setType(String type)
	{
		this.type = type;
	}
	public ArrayList<Site> getSite()
	{
		return site;
	}
	public void setSite(ArrayList<Site> site)
	{
		this.site = site;
	}
	public Table getTable()
	{
		return table;
	}

	public void setTable(Table table)
	{
		this.table = table;
	}
}