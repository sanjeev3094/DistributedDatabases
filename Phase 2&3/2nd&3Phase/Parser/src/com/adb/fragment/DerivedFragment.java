package com.adb.fragment;

import java.util.ArrayList;

import com.adb.general.Site;
import com.adb.general.Table;

public class DerivedFragment extends Fragment
{
	HorizontalFragment joinHFrag;
	String semiJoinAttr;

	public DerivedFragment()
	{
		super();
	}

	public DerivedFragment(String name, String type, ArrayList<Site> site,Table table, HorizontalFragment joinHFrag, String semiJoinAttr)
	{
		super(name, type, site, table);
		this.joinHFrag = joinHFrag;
		this.semiJoinAttr = semiJoinAttr;
	}

	public HorizontalFragment getJoinHFrag()
	{
		return joinHFrag;
	}

	public void setJoinHFrag(HorizontalFragment joinHFrag)
	{
		this.joinHFrag = joinHFrag;
	}

	public String getSemiJoinAttr()
	{
		return semiJoinAttr;
	}

	public void setSemiJoinAttr(String semiJoinAttr)
	{
		this.semiJoinAttr = semiJoinAttr;
	}
}
