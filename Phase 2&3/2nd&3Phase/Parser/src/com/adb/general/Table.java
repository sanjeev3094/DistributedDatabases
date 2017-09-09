package com.adb.general;

import com.adb.select.SelectStatement;

public class Table extends SQLStatement 
{
	private String name;
	private String aliasName;
	private TableType type;
	private SelectStatement selstmt;

	public static void copy(Table from,Table to)
	{
		to.setName(from.getName());
		to.setAlias(from.getAlias());
		to.setType(from.getType());
	}

	public String getName()
	{
		return this.name;
	}
	public String getAlias()
	{
		return this.aliasName;
	}
	public TableType getType()
	{
		return this.type;
	}
	public void setName(String name)
	{
		this.name=name;
	}
	public void setAlias(String aname)
	{
		this.aliasName=aname;
	}
	public void setType(TableType type)
	{
		this.type=type;
		if(type==TableType.NORMAL)
		{
			this.selstmt=null;
		}
	}

	
}
