package com.adb.select;

import com.adb.general.SQLStatement;
import com.adb.general.WhereCondition;

public class SelectStatement extends SQLStatement
{
	private SelectAttrList selattr;
	private FromTableList tablelist;
	private WhereCondition wherecondition;
	private GroupByCondition groupbycondition;
	private HavingCondition havingcondition;
	private OrderByCondition orderbycondition;
	
	public SelectStatement()
	{
		
	}
	public SelectAttrList getAttrList()
	{
		return this.selattr;
	}
	public void setAttrList(SelectAttrList attrlist)
	{
		this.selattr=attrlist;
	}
	public FromTableList getTableList()
	{
		return this.tablelist;
	}
	public void setTableList(FromTableList attrlist)
	{
		this.tablelist=attrlist;
	}
	public void setWhereCondition(WhereCondition wc)
	{
		this.wherecondition=wc;
	}
	public WhereCondition getWhereCondition()
	{
		return this.wherecondition;
	}
	public void setGroupByCondition(GroupByCondition gbc)
	{
		this.groupbycondition=gbc;
	}
	public GroupByCondition getGroupByCondition()
	{
		return this.groupbycondition;
	}
	public void setHavingCondition(HavingCondition hc)
	{
		this.havingcondition=hc;
	}
	public HavingCondition getHavingCondition()
	{
		return this.havingcondition;
	}
	public void setOrderByCondition(OrderByCondition obc)
	{
		this.orderbycondition=obc;
	}
	public OrderByCondition getOrderByCondition()
	{
		return this.orderbycondition;
	}
	public String toString()
	{
	    StringBuffer stmt = new StringBuffer();
	    stmt.append("SELECT ");
	    stmt.append(selattr.toString());
    	//stmt.append("\n");
	    stmt.append(" FROM ");
	    stmt.append(tablelist.toString());
	    if(this.wherecondition!=null)
	    {
	    	//stmt.append("\n");
	    	stmt.append(" WHERE ");
	    	stmt.append(wherecondition.toString());
	    }
	    if(this.groupbycondition!=null)
	    {
	    	//stmt.append("\n");
	    	stmt.append(" GROUP BY ");
	    	stmt.append(groupbycondition.toString());
	    }
	    if(this.havingcondition!=null)
	    {
	    	//stmt.append("\n");
	    	stmt.append(" HAVING ");
	    	stmt.append(havingcondition.toString());
	    }
	    if(this.orderbycondition!=null)
	    {
	    	//stmt.append("\n");
	    	stmt.append(" ORDER BY ");
	    	stmt.append(orderbycondition.toString());
	    }
	    //stmt.append(";");
	    
		return stmt.toString();
	}
}
