package com.adb.Executor;

import java.io.Serializable;
import java.util.HashMap;

import com.adb.Executor.SDD1.FragmentStatistics;

public class Statistics implements Serializable
{
	private static final long serialVersionUID = 1L;
	HashMap<String,Integer> VRAttrMap;
	HashMap<String,FragmentStatistics> fragmentStatMap;
	
	public HashMap<String, Integer> getVRAttrMap()
	{
		return VRAttrMap;
	}
	public void setVRAttrMap(HashMap<String, Integer> vRAttrMap)
	{
		VRAttrMap = vRAttrMap;
	}
	public HashMap<String, FragmentStatistics> getFragmentStatMap()
	{
		return fragmentStatMap;
	}
	public void setFragmentStatMap(HashMap<String, FragmentStatistics> fragmentStatMap)
	{
		this.fragmentStatMap = fragmentStatMap;
	}
}
