package com.adb.update;

import com.adb.general.SetValueList;
import com.adb.general.WhereCondition;
import com.adb.select.FromTableList;

public class UpdateStatement 
{
	private FromTableList tablelist;
	private SetValueList setvaluelist; 
	private WhereCondition wherecondition;
	
	public FromTableList getTableList()
	{
		return this.tablelist;
	}
	public void setTableList(FromTableList attrlist)
	{
		this.tablelist=attrlist;
	}
	public SetValueList getSetValueList()
	{
		return this.setvaluelist;
	}
	public void setValueList(SetValueList setvallist)
	{
		this.setvaluelist=setvallist;
	}
	public void setWhereCondition(WhereCondition wc)
	{
		this.wherecondition=wc;
	}
	public WhereCondition getWhereCondition()
	{
		return this.wherecondition;
	}
	public String toString()
	{
	    StringBuffer stmt = new StringBuffer();
	    stmt.append("UPDATE ");
	    stmt.append(tablelist.toString());
	    stmt.append(" SET ");
	    stmt.append(setvaluelist.toString());
	    if(this.wherecondition!=null)
	    {
	    	stmt.append(" WHERE ");
	    	stmt.append(wherecondition.toString());
	    }
	    stmt.append(";");

	    return stmt.toString();
	}
}
