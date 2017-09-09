package com.adb.select;

import java.util.ArrayList;

import com.adb.general.Attribute;
import com.adb.general.Order;

public class GroupByCondition 
{
	ArrayList<Attribute> attrlist;
	ArrayList<Order> orderlist;
	
	public GroupByCondition()
	{
		this.attrlist=new ArrayList<Attribute>();
		this.orderlist=new ArrayList<Order>();
	}
	public ArrayList<Attribute> getAttrList()
	{
		return this.attrlist;
	}
	public void setOrder(ArrayList<Order> orderlist)
	{
		this.orderlist=orderlist;
	}
	public ArrayList<Order> getOrder()
	{
		return this.orderlist;
	}
	public void setAttrList(ArrayList<Attribute> attrlist)
	{
		this.attrlist=attrlist;
	}

	public String toString()
	{
		String delim=",";
	    StringBuffer flds = new StringBuffer();
	    for(int i=0;i<attrlist.size();i++)
	    {
	    	if (i>0) 
	    		flds.append(delim);
	    	String tmp=attrlist.get(i).getTable().getName()+"."+attrlist.get(i).getName();
	        if(attrlist.get(i).getFunction()!=null)
	        	flds.append(attrlist.get(i).getFunction().toUpperCase()+"("+tmp+")");
	        else
		        flds.append(tmp);
	        if(orderlist.get(i)==Order.ASCENDING)
	        	flds.append(" ASC");
	        else
	        	flds.append(" DESC");
	    }
		return flds.toString();
	}

	public String toStringWithoutTable()
	{
		String delim=",";
	    StringBuffer flds = new StringBuffer();
	    for(int i=0;i<attrlist.size();i++)
	    {
	    	if (i>0) 
	    		flds.append(delim);
	    	String tmp=attrlist.get(i).getName();
	        if(attrlist.get(i).getFunction() != null)
	        	flds.append(attrlist.get(i).getFunction().toUpperCase()+"("+tmp+")");
	        else
		        flds.append(tmp);
	        
	        if(orderlist.get(i) == Order.ASCENDING)
	        	flds.append(" ASC");
	        else
	        	flds.append(" DESC");
	    }
		return flds.toString();
	}

}
