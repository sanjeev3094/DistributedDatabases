package com.adb.general;


public class Attribute 
{
	private String name;
	private String aliasName;
	private String type;
	private String function;
	private Table table;
	
	public Attribute()
	{
		super();
	}
	public Attribute(String name, String aliasName, String type,String function, Table table)
	{
		super();
		this.name = name;
		this.aliasName = aliasName;
		this.type = type;
		this.function = function;
		this.table = table;
	}
	public void copy(Attribute from,Attribute to)
	{
		to.setAlias(from.getAlias());
		to.setName(from.getName());
		to.setFunction(from.getFunction());
		to.setType(from.getType());
		Table.copy(from.getTable(),to.getTable());
	}
	public String getName()
	{
		return this.name;
	}
	public String getAlias()
	{
		return this.aliasName;
	}
	public String getType()
	{
		return this.type;
	}
	public Table getTable()
	{
		return this.table;
	}
	public String getFunction()
	{
		return this.function;
	}
	public void setFunction(String funct)
	{
		this.function=funct;
	}
	public void setName(String name)
	{
		this.name=name;
	}
	public void setAlias(String aname)
	{
		this.aliasName=aname;
	}
	public void setType(String type)
	{
		this.type=type;
	}
	public void setTable(Table table)
	{
		this.table=table;
	}
}
