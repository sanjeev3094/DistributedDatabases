package com.adb.Executor.SDD1;

import java.io.Serializable;
import java.util.ArrayList;

public class FragmentStatistics implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	String name;
	Integer noOfTuples;
	Integer sizeOfTuple;
	ArrayList<String> attrList;
	ArrayList<Integer> attrSizeList;
	
	public FragmentStatistics()
	{
		super();
	}

	public FragmentStatistics(String name, Integer noOfTuples, Integer sizeOfTuple, ArrayList<String> attrList,
			ArrayList<Integer> attrSizeList)
	{
		super();
		this.name = name;
		this.noOfTuples = noOfTuples;
		this.sizeOfTuple = sizeOfTuple;
		this.attrList = attrList;
		this.attrSizeList = attrSizeList;
	}


	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public Integer getNoOfTuples()
	{
		return noOfTuples;
	}

	public void setNoOfTuples(Integer noOfTuples)
	{
		this.noOfTuples = noOfTuples;
	}

	public Integer getSizeOfTuple()
	{
		return sizeOfTuple;
	}

	public void setSizeOfTuple(Integer sizeOfTuple)
	{
		this.sizeOfTuple = sizeOfTuple;
	}

	public ArrayList<String> getAttrList()
	{
		return attrList;
	}

	public void setAttrList(ArrayList<String> attrList)
	{
		this.attrList = attrList;
	}

	public ArrayList<Integer> getAttrSizeList()
	{
		return attrSizeList;
	}

	public void setAttrSizeList(ArrayList<Integer> attrSizeList)
	{
		this.attrSizeList = attrSizeList;
	}
}
