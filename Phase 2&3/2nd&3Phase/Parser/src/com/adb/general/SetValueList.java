package com.adb.general;

import java.util.ArrayList;

import com.adb.select.SelectAttrList;

public class SetValueList extends SelectAttrList
{
	SelectAttrList attriblist;
	ArrayList<String> valuelist;
	ArrayList<WhereCondition> setlist;
	
	public SetValueList()
	{
		this.attriblist=new SelectAttrList();
		this.valuelist=new ArrayList<String>();
		this.setlist=new ArrayList<WhereCondition>();
	}
	
	public ArrayList<Attribute> getAttrList()
	{
		return this.attriblist.getAttrList();
	}
	public void setAttrList(ArrayList<Attribute> attrlist)
	{
		this.attriblist.setAttrList(attrlist);
	}
	public ArrayList<String> getValueList()
	{
		return this.valuelist;
	}
	public void setValueList(ArrayList<String> valuelist)
	{
		this.valuelist=valuelist;
	}
	public ArrayList<WhereCondition> getSetList()
	{
		return this.setlist;
	}
	public void setSetList(ArrayList<WhereCondition> setlist)
	{
		this.setlist=setlist;
	}
	public String toString()
	{
		String delim=",";
	    StringBuffer flds = new StringBuffer();
	    ArrayList<Attribute> attrlist=getAttrList();
	    for(int i=0;i<attrlist.size();i++)
	    {
	    	if (i>0) 
	    		flds.append(delim);
	    	String tmp=attrlist.get(i).getTable().getName()+"."+attrlist.get(i).getName();
	        if(attrlist.get(i).getFunction()!=null)
	        	flds.append(attrlist.get(i).getFunction().toUpperCase()+"("+tmp+")");
	        else
		        flds.append(tmp);
	        //if(attrlist.get(i).getAlias()!=null)
	        //flds.append(" "+attrlist.get(i).getAlias());
	        flds.append(" = "+valuelist.get(i).toString());
	    }
		return flds.toString();
	}


}
