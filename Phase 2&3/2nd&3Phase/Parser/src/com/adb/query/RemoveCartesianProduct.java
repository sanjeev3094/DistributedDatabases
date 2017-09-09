package com.adb.query;

import java.util.ArrayList;
import java.util.HashMap;

import com.adb.fragment.OptimizeFragmentQuery;
import com.adb.general.Condition;
import com.adb.general.Perm;
import com.adb.general.Table;
import com.adb.select.FromTableList;
import com.adb.select.SelectStatement;

public class RemoveCartesianProduct 
{
	public static HashMap<String,ArrayList<String>> joinMap=new HashMap<String,ArrayList<String>>();
	public static void removeCP(SelectStatement slc)
	{
		if(OptimizeFragmentQuery.queryJoinCond.containsKey(slc.toString()))
		{
			ArrayList<Condition> arrcond= OptimizeFragmentQuery.queryJoinCond.get(slc.toString());
			for(Condition cond:arrcond)
			{
//			   System.out.println(cond.getLHS()+" "+cond.getOperator()+" "+cond.getRHS());
			   String lhs=cond.getLHS().split("\\.")[0];
			   String rhs=cond.getRHS().split("\\.")[0];
//			   System.out.println(lhs+" "+rhs);
			   if(!joinMap.containsKey(lhs))
			   {
				   ArrayList<String> arr=new ArrayList<String>();
				   arr.add(rhs);
				   joinMap.put(lhs,arr);
			   }
			   else
			   {
				   ArrayList<String> arr=joinMap.get(lhs);
				   arr.add(rhs);
				   joinMap.put(lhs,arr);
			   }
				   
			}
	        FromTableList ftl=slc.getTableList();
	        ArrayList<Table> tab=ftl.getTableList();
	        int size=tab.size();
	        String[] tabArray=new String[size];
	        for(int i=0;i<size;i++)
	           tabArray[i]=tab.get(i).getName();

	        Perm p=new Perm(tabArray);
	        String[][] res=p.callGenPerm();
/*	        for(String sstr[]:res)
	        {
	        	for(String str:sstr)
	        		System.out.print(str+" ");
	        	System.out.println();
	        }
*/	        boolean[] bb=new boolean[res.length];
	        for(int i=0;i<res.length;i++)
	        {
	        	String[] sstr=res[i];
	        	bb[i]=false;
	        	for(int j=0;j<sstr.length-1;j++)
	        	{
	        		if(joinMap.containsKey(sstr[j]) && joinMap.get(sstr[j]).contains(sstr[j+1]))
	        			bb[i]|=true;
	        	//	else
	        		//	bb[i]|=false;
	        	}
/*	        	if(bb[i])
	        	{
	        		for(String str:sstr)
	        			System.out.print(str+" ");
	        		System.out.println(" : "+bb[i]);
	        	}
*/	        }
		}
	}
}
