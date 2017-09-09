package com.adb.query;

import java.util.ArrayList;

import com.adb.general.SQLStatement;
import com.adb.select.SelectAttrList;
import com.adb.select.SelectStatement;

public class OptimizedSelectQuery extends OptimizedQuery
{
	boolean countFlag;
	boolean distinctFlag;
	ArrayList<SelectStatement> stmtList;
	SelectAttrList topattrlist;
	
	public OptimizedSelectQuery() 
	{
		countFlag=false;
		distinctFlag=false;
		stmtList=null;
	}
	public boolean isCount()
	{
		return this.countFlag;
	}
	public boolean isDistinct()
	{
		return this.distinctFlag;
	}
	public void setCountFlag(boolean flag)
	{
		this.countFlag=flag;
	}
	public void setDistinctFlag(boolean flag)
	{
		this.distinctFlag=flag;
	}
	public ArrayList<SelectStatement> getAttrList()
	{
		return this.stmtList;
	}
	@SuppressWarnings("unchecked")
	public void setAttrList(ArrayList<SelectStatement>  arrselstmt)
	{
		this.stmtList =(ArrayList<SelectStatement>)arrselstmt.clone();
	}
	public SelectAttrList getTopAttrList()
	{
		return this.topattrlist;
	}
	public void setTopAttrList(SelectAttrList  attrlist)
	{
		this.topattrlist =(SelectAttrList)attrlist;
	}

	public String toString()
	{
		StringBuffer stmt = new StringBuffer();
		if(topattrlist.getAttrList().size()>0)
		{
			stmt.append("SELECT ");
/*		    if(distinctFlag==true)
			    stmt.append("DISTINCT ");
*/		    stmt.append(topattrlist.toString());
		    stmt.append("\n");
		    stmt.append("FROM ");
		    stmt.append("\n");
		}

		else if(countFlag==true)
		{
		    stmt.append("SELECT ");
		    if(distinctFlag==true)
			    stmt.append("DISTINCT ");
		    stmt.append("count(*)");
		    stmt.append("\n");
		    stmt.append("FROM ");
		    stmt.append("\n");
		}
		int j=0;
		for(SQLStatement selst:stmtList)
		{
			if(j>0)
				stmt.append("UNION\n");
			stmt.append(selst.toString());
			stmt.append("\n");
			++j;
		}
		return stmt.toString();
	}

}
