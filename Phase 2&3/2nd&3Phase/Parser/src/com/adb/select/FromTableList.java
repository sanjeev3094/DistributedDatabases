package com.adb.select;

import java.util.ArrayList;

import com.adb.general.Table;

public class FromTableList
{
	ArrayList<Table> tablelist;

	public FromTableList()
	{
		this.tablelist = new ArrayList<Table>();
	}

	public void setList(ArrayList<Table> list)
	{
		this.tablelist = list;
	}

	public ArrayList<Table> getTableList()
	{
		return this.tablelist;
	}

	public String toString()
	{
		String delim = ",";
		StringBuffer flds = new StringBuffer();
		for (int i = 0; i < tablelist.size(); i++)
		{
			if (i > 0)
				flds.append(delim);
			flds.append(tablelist.get(i).getName());
			if (tablelist.get(i).getAlias() != null)
				flds.append(" " + tablelist.get(i).getAlias());
		}
		return flds.toString();
	}

}
