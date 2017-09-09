package com.adb.Executor.SDD1;

import java.io.Serializable;
import java.util.HashMap;

public class DatabaseProfile implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	public HashMap<String,Double> relationSizeMap = new HashMap<String, Double>();
	public HashMap<String, SelectivityFactorAndSizeAttr> attrDetailMap = new HashMap<String, SelectivityFactorAndSizeAttr>();
	
	public HashMap<String, Double> getRelationSizeMap()
	{
		return relationSizeMap;
	}
	public void setRelationSizeMap(HashMap<String, Double> relationSizeMap)
	{
		this.relationSizeMap = relationSizeMap;
	}
	public HashMap<String, SelectivityFactorAndSizeAttr> getAttrDetailMap()
	{
		return attrDetailMap;
	}
	public void setAttrDetailMap(HashMap<String, SelectivityFactorAndSizeAttr> attrDetailMap)
	{
		this.attrDetailMap = attrDetailMap;
	}
}
