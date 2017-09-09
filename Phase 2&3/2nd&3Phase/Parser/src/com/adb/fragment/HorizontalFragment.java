package com.adb.fragment;

import java.util.ArrayList;

import com.adb.general.Site;
import com.adb.general.Table;

public class HorizontalFragment extends Fragment
{
	String attr;
	String operator;
	String val;

	public HorizontalFragment()
	{
		super();
	}

	public HorizontalFragment(String name, String type,ArrayList<Site> site, Table table, String attr,String operator, String val)
	{
		super(name, type, site, table);
		this.attr = attr;
		this.operator = operator;
		this.val = val;
	}

	public String getAttr()
	{
		return attr;
	}

	public void setAttr(String attr)
	{
		this.attr = attr;
	}

	public String getOperator()
	{
		return operator;
	}

	public void setOperator(String operator)
	{
		this.operator = operator;
	}

	public String getVal()
	{
		return val;
	}

	public void setVal(String val)
	{
		this.val = val;
	}
}
