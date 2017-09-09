package com.adb.Executor.SDD1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.adb.Executor.ShellProcess;
import com.adb.Executor.loadDBProfile.ExecuteLoaderUtility;
import com.adb.Executor.loadDBProfile.LoadDBProfile;
import com.adb.fragment.Fragment;
import com.adb.fragment.OptimizeFragmentQuery;
import com.adb.general.Condition;
import com.adb.general.Constants;
import com.adb.general.Site;
import com.adb.general.Table;
import com.adb.parse.Parser;
import com.adb.select.FromTableList;
import com.adb.select.SelectStatement;
import com.adb.utils.SemanticUtility;

public class QueryExecutor
{
	public static HashMap<String, String> relationRelIdMap = new HashMap<String, String>();
	public static String FINAL_TABLE_ATTR_LIST;
	public static String FINAL_TABLE_GROUP_LIST;
	public static String FINAL_TABLE_ORDERBY_LIST;

	public void findSitesForFragment(ArrayList<Table> tableList)
	{
		// find the fragments located on other sites
		/*
		 * for(Table table:tableList) { String tableName = table.getName();
		 * 
		 * }
		 */
	}
	
	public void showGroupQuery(int cnt, String groupby)
	{
		ShellProcess sp = new ShellProcess();
		String query = "";
		
		// create a temporary table 
		
		query = "create table tmp("+QueryExecutor.FINAL_TABLE_ATTR_LIST+");\n";
		
		query += "insert into tmp ";
		for (int i = 1; i < cnt; ++i)
		{
			if (i > 1)
			{
				query += " union ";
			}
			query =query+ " select * from " + Constants.RESULT_TABLE_NAME + i;
		}
		String[] attrs=QueryExecutor.FINAL_TABLE_ATTR_LIST.split(",");
		String finalattrstr="";
		ArrayList<String> avgattr=new ArrayList<String>();
		for (int i=0;i<attrs.length;i++)
		{
			if(i>0)
				finalattrstr+=",";
			String[] attrtype=attrs[i].split(" ");
			if(attrs[i].contains("_Z_"))
			{
				String[] attr=attrtype[0].split("_Z_");
				if(attr[0].toLowerCase().startsWith("count"))
					finalattrstr+="sum("+attrtype[0]+")";
				else if(attr[0].toLowerCase().startsWith("avg"))
				{
					finalattrstr+=attr[0]+"("+attrtype[0]+")";
					avgattr.add(attr[1]);
				}
				else
					finalattrstr+=attr[0]+"("+attrtype[0]+")";
			}
			else
				finalattrstr+=attrtype[0];

		}
		for(int i=0;i<avgattr.size();i++)
		{
			finalattrstr+=",";
			finalattrstr+="(SUM(SUM_Z_"+avgattr.get(i)+")/SUM(COUNT_Z_"+avgattr.get(i)+")) AVG_Z_"+avgattr.get(i);
		}
		if(Parser.distnctflag==true)
			query += ";\nselect distinct "+finalattrstr+" from tmp ";
		else
			query += ";\nselect "+finalattrstr+" from tmp ";
		
		query += "group by " + groupby;
		
		if(FINAL_TABLE_ORDERBY_LIST != null)
			query += " order by " + FINAL_TABLE_ORDERBY_LIST;
		query += ";\n";
			
		System.out.println("Final q= "+query);
		
		try
		{
			sp.execQuery(Constants.USER_NAME, Constants.PASSWORD, null,Constants.RESULT_SCHEMA_NAME,query);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}

	}

	
	public void showGroupQueryResult(int cnt, String groupBy)
	{
		if(cnt==1)
			return;
		
		ShellProcess sp = new ShellProcess();
		String query = "";

		// create a temporary table
		query = "create table tmp select * from "+Constants.RESULT_TABLE_NAME+1 +";\n";
		
		query += "insert into tmp ";
		for (int i = 2; i < cnt; ++i)
		{
			if (i > 2)
			{
				query += " union ";
			}
			query = query+ " select * from " + Constants.RESULT_TABLE_NAME + i;
		}
		query += ";\nselect * from tmp group by "+ groupBy +";\n";
		
		try
		{
			sp.execQuery(Constants.USER_NAME, Constants.PASSWORD, null,Constants.RESULT_SCHEMA_NAME,query);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}
	
	public void showCountQueryResult(int cnt)
	{
		ShellProcess sp = new ShellProcess();
		String query = "";
		
		// create a temporary table 
		
		query = "create table tmp(c integer);\n";
		
		query += "insert into tmp ";
		for (int i = 1; i < cnt; ++i)
		{
			if (i > 1)
			{
				query += " union ";
			}
			query =query+ " select * from " + Constants.RESULT_TABLE_NAME + i;
		}
		query += ";\nselect sum(c) count from tmp;\n";
		
		try
		{
			sp.execQuery(Constants.USER_NAME, Constants.PASSWORD, null,Constants.RESULT_SCHEMA_NAME,query);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}
	public void showNoGroupQuery(int cnt)
	{
		ShellProcess sp = new ShellProcess();
		String query = "";
		
		// create a temporary table 
		
		query = "create table tmp("+QueryExecutor.FINAL_TABLE_ATTR_LIST+");\n";
		
		query += "insert into tmp ";
		for (int i = 1; i < cnt; ++i)
		{
			if (i > 1)
			{
				query += " union ";
			}
			query =query+ " select * from " + Constants.RESULT_TABLE_NAME + i;
		}
		String[] attrs=QueryExecutor.FINAL_TABLE_ATTR_LIST.split(",");
		String finalattrstr="";
		ArrayList<String> avgattr=new ArrayList<String>();
		for (int i=0;i<attrs.length;i++)
		{
			if(i>0)
				finalattrstr+=",";
			String[] attrtype=attrs[i].split(" ");
			if(attrs[i].contains("_Z_"))
			{
				String[] attr=attrtype[0].split("_Z_");
				if(attr[0].toLowerCase().startsWith("count"))
					finalattrstr+="sum("+attrtype[0]+")";
				else if(attr[0].toLowerCase().startsWith("avg"))
				{
					finalattrstr+=attr[0]+"("+attrtype[0]+")";
					avgattr.add(attr[1]);
				}
				else
					finalattrstr+=attr[0]+"("+attrtype[0]+")";
			}
			else
				finalattrstr+=attrtype[0];

		}
		for(int i=0;i<avgattr.size();i++)
		{
			finalattrstr+=",";
			finalattrstr+="(SUM(SUM_Z_"+avgattr.get(i)+")/SUM(COUNT_Z_"+avgattr.get(i)+")) AVG_Z_"+avgattr.get(i);
		}
		if(Parser.distnctflag==true)
			query += ";\nselect distinct "+finalattrstr+" from tmp";
		else
			query += ";\nselect "+finalattrstr+" from tmp";
		
		if(FINAL_TABLE_ORDERBY_LIST != null)
			query += " order by " + FINAL_TABLE_ORDERBY_LIST;
		
		query += ";\n";
		System.out.println("Final q= "+query);
		
		try
		{
			sp.execQuery(Constants.USER_NAME, Constants.PASSWORD, null,Constants.RESULT_SCHEMA_NAME,query);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}

	}
	public void showSimpleQueryResult(int cnt)
    {
		System.out.println("Count= "+cnt);
		ShellProcess sp = new ShellProcess();
		String query = "";
		for (int i = 1; i < cnt; ++i)
		{
			if (i > 1)
			{
				query += " union ";
			}
			query =query+ " select * from " + Constants.RESULT_TABLE_NAME + i;
		}
		System.out.println("query : " + query);
		try
		{
			sp.execQuery(Constants.USER_NAME, Constants.PASSWORD, null,Constants.RESULT_SCHEMA_NAME,query);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
    }


	void loadAllDBProfileinFile()
	{
		ExecuteLoaderUtility exec = new ExecuteLoaderUtility();
		ArrayList<String> fragList = exec.loadFragmentListFromDB();
		SemanticUtility.getAllFragmentsMap(fragList);
		exec.calculateDatabaseProfile(fragList);
		ExecuteLoaderUtility.serializeDBProfile();
	}

	public void reduceRelationUsingWherePredicate(ArrayList<Condition> attrConditions, ArrayList<Table> tableList)
	{
		HashMap<String, String> relPredicateMap = new HashMap<String, String>();
		ShellProcess shellProcess = new ShellProcess();

		if(attrConditions != null)
		{
			for (Condition cond:attrConditions)
			{
				String lhs = cond.getLHS();
				System.out.println(lhs);
				String r[] = lhs.split("\\.");
				String table = r[0];
	
				String predicate = lhs + cond.getOperator() + cond.getRHS();
	
				if (relPredicateMap.containsKey(table))
				{
					String newPredicate = relPredicateMap.get(table) + " AND " + predicate;
					relPredicateMap.put(table, newPredicate);
				}
				else
					relPredicateMap.put(table, predicate);
			}
		}
		try
		{
			for (Map.Entry<String, String> entry : relPredicateMap.entrySet())
			{
				String relation = entry.getKey();
				String predicate = entry.getValue();

				System.out.println(relation + " : " + predicate);
				Fragment relObj = SemanticUtility.globalFragmentMap.get(relation);
				Site site = relObj.getSite().get(0);
				shellProcess.getReducedWhereRelation(site.getUserName(), site.getPasswd(), site.getSiteName() + "@" + site.getIpAddress(),Constants.COLLEGE_SCHEMA, relation, predicate, Constants.TEMP_SCHEMA_NAME, relation);
			}

			// for those relation which don't have where clause
			for (Table table : tableList)
			{
				String relation = table.getName();
				System.out.println(relation);
				Fragment relObj = SemanticUtility.globalFragmentMap.get(relation);
				Site site = relObj.getSite().get(0);

				if (!relPredicateMap.containsKey(relation))
				{
					shellProcess.getReducedWhereRelation(site.getUserName(), site.getPasswd(), site.getSiteName() + "@" + site.getIpAddress(),Constants.COLLEGE_SCHEMA, relation, null, Constants.TEMP_SCHEMA_NAME, relation);
				}
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}

	public void populateTablesofSDD(SelectStatement slc, int cnt)
	{
		// loadAllDBProfileinFile();

		FromTableList fromList = slc.getTableList();
		ArrayList<Table> tableList = fromList.getTableList();
//		findSitesForFragment(tableList);
		ArrayList<String> relations = new ArrayList<String>();
		
		for(int i=0;i<tableList.size();++i)
			relations.add(tableList.get(i).getName());
		
		ArrayList<Condition> joinConditions = OptimizeFragmentQuery.queryJoinCond.get(slc.toString());
		ArrayList<Condition> attrConditions = OptimizeFragmentQuery.queryAttributeCond.get(slc.toString());

		//System.out.println("Size of attrCondition : " + attrConditions.size());
		//if(attrConditions != null)
		reduceRelationUsingWherePredicate(attrConditions, tableList);

		ArrayList<String> totalList = new ArrayList<String>();

		System.out.println("SDD-1 starts...");
		System.out.println(slc.toString());

		for (int i=0;i<joinConditions.size();++i)
		{
			Condition cond = joinConditions.get(i);
			String lhs = cond.getLHS();
			String rhs = cond.getRHS();
			System.out.println(lhs + " = " + rhs);
			String[] l = lhs.split("\\.");
			String[] r = rhs.split("\\.");
			totalList.add(lhs + Constants.SEMIJOIN_SEP + rhs);
			totalList.add(rhs + Constants.SEMIJOIN_SEP + lhs);

			if (!relations.contains(l[0]))
				relations.add(l[0]);

			if (!relations.contains(r[0]))
				relations.add(r[0]);
		}

		DatabaseProfile dbProfile = new DatabaseProfile();

		HashMap<String, Double> relationALLMap = LoadDBProfile.dbProfileALL.getRelationSizeMap();
		HashMap<String, SelectivityFactorAndSizeAttr> attrDetALLMAp = LoadDBProfile.dbProfileALL.getAttrDetailMap();

		for (String rel : relations)
		{
			// System.out.println(rel);
			dbProfile.getRelationSizeMap().put(rel, relationALLMap.get(rel));
			System.out.println("rel "+ rel);
		}

		System.out.println(dbProfile.getRelationSizeMap().size());
		
		for (String tot : totalList)
		{
			// System.out.println(tot);
			dbProfile.getAttrDetailMap().put(tot, attrDetALLMAp.get(tot));
		}

		 SDDImplementor sdd = new SDDImplementor();
		 sdd.initiateSDD(dbProfile, totalList);
		 
		 sdd.executeFragmentQuery(slc,cnt);
	}
}
