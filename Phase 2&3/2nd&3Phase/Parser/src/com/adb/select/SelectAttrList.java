package com.adb.select;

import java.util.ArrayList;

import com.adb.general.Attribute;
import com.adb.parse.Parser;

public class SelectAttrList
{
	ArrayList<Attribute> attrlist;

	public SelectAttrList()
	{
		this.attrlist = new ArrayList<Attribute>();
	}

	public ArrayList<Attribute> getAttrList()
	{
		return this.attrlist;
	}

	public void setAttrList(ArrayList<Attribute> attrlist)
	{
		this.attrlist = attrlist;
	}

	public String toString()
	{
		String delim = ",";
		StringBuffer flds = new StringBuffer();
		int i=0;
		if(Parser.distnctflag==true)
		{
			flds.append("DISTINCT ");
			
		}
		for (i = 0; i < attrlist.size(); i++)
		{
			if (i > 0)
				flds.append(delim);
			String tmp = attrlist.get(i).getTable().getName() + "."+ attrlist.get(i).getName();
			if (attrlist.get(i).getFunction() != null)
				flds.append(attrlist.get(i).getFunction().toUpperCase() + "("+ tmp + ")");
			else
				flds.append(tmp);
			if (attrlist.get(i).getAlias() != null)
				flds.append(" " + attrlist.get(i).getAlias());
		}
		if(Parser.cntflag==true)
		{
			if (i>0)
				flds.append(delim);
			flds.append("count(*)");
			
		}
		return flds.toString();
	}
}
