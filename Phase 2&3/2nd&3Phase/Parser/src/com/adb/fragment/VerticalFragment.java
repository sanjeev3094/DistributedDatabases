package com.adb.fragment;

import java.util.ArrayList;

import com.adb.general.Attribute;
import com.adb.general.Site;
import com.adb.general.Table;

public class VerticalFragment extends Fragment
{
	ArrayList<Attribute> attrList;
	Attribute primaryKey;

	public VerticalFragment()
	{
		super();
	}

	public VerticalFragment(String name, String type,ArrayList<Site> site, Table table,ArrayList<Attribute> attrList,Attribute primaryKey)
	{
		super(name, type, site, table);
		this.attrList = attrList;
		this.primaryKey = primaryKey;
	}

	public ArrayList<Attribute> getAttrList()
	{
		return attrList;
	}

	public void setAttrList(ArrayList<Attribute> attrList)
	{
		this.attrList = attrList;
	}

	public Attribute getPrimaryKey()
	{
		return primaryKey;
	}

	public void setPrimaryKey(Attribute primaryKey)
	{
		this.primaryKey = primaryKey;
	}
}
