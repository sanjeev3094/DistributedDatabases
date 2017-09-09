package com.adb.fragment;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import gudusoft.gsqlparser.stmt.TUpdateSqlStatement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.adb.general.Attribute;
import com.adb.general.Condition;
import com.adb.general.Constants;
import com.adb.general.ModifyWhere;
import com.adb.general.Order;
import com.adb.general.SetValueList;
import com.adb.general.Table;
import com.adb.general.WhereCondition;
import com.adb.parse.Parser;
import com.adb.select.FromTableList;
import com.adb.select.GroupByCondition;
import com.adb.select.HavingCondition;
import com.adb.select.OrderByCondition;
import com.adb.select.SelectAttrList;
import com.adb.select.SelectStatement;
import com.adb.update.UpdateStatement;
import com.adb.utils.SemanticUtility;

public class DataLocalisation
{
	String JOIN = ",";
	public static ArrayList<String> queryEnumerationList = new ArrayList<String>();
	public static ArrayList<Fragment> fromFragmentList = new ArrayList<Fragment>();
	public static ArrayList<SelectStatement> selStatements = new ArrayList<SelectStatement>();

	public static HashMap<String,ArrayList<Fragment>> localTableFragMap = new HashMap<String, ArrayList<Fragment>>();
	public static HashMap<String, TreeMap<String, Integer>> verticalFragStateMap = new HashMap<String, TreeMap<String, Integer>>();
	private static EDbVendor dbv = EDbVendor.dbvmysql;
	private static TGSqlParser parser1 = new TGSqlParser(dbv);

	
	public void enumerateQuery(SelectStatement stmt)
	{
		ArrayList<Table> tableList = stmt.getTableList().getTableList();
		SemanticUtility.getFragmentsListForFromTables(tableList);
	}
	public ArrayList<String> populateMaps(String from,ArrayList<String> notPresVertFrag)
	{
		String fromFragments[] = from.split(JOIN);
		ArrayList<String> fromFragList = new ArrayList<String>();
		
		// Find the table corresponding to the fragment name and the Fragment
		// Object and form a local Table Fragment Map
		for (String fName : fromFragments)
		{
			if(!notPresVertFrag.isEmpty() && notPresVertFrag.contains(fName))
			{
				continue;
			}
			fromFragList.add(fName);
			String tableName = SemanticUtility.fragmentTableMap.get(fName)
					.getName();
			Fragment fragObj = SemanticUtility.globalFragmentMap.get(fName);

			if (localTableFragMap.containsKey(tableName))
			{
				ArrayList<Fragment> fragList = localTableFragMap.get(tableName);
				fragList.add(fragObj);
				localTableFragMap.put(tableName, fragList);
			}
			else
			{
				ArrayList<Fragment> fList = new ArrayList<Fragment>();
				fList.add(fragObj);
				localTableFragMap.put(tableName, fList);
			}
		}
		
		return fromFragList;
	}

	public void setVerticalFlagMap(String tableName,String attrName)
	{
		ArrayList<Fragment> fragList = SemanticUtility.tableFragmentListMap.get(tableName);

		for (Fragment frag : fragList)
		{
			if (frag.getType().equalsIgnoreCase(Constants.VER_FRAG_TYPE))
			{
				// check attribute present in which vertical fragment and
				// replace it wisely
				VerticalFragment vfrag = (VerticalFragment) frag;
				ArrayList<Attribute> attrList = vfrag.getAttrList();

				for (Attribute attrObj : attrList)
				{
					String fragAttr = attrObj.getName();
					if (fragAttr.equalsIgnoreCase(attrName))
					{
						if(verticalFragStateMap.containsKey(tableName))
						{
							TreeMap<String, Integer> hmap = verticalFragStateMap.get(tableName);
							
							if(!vfrag.getPrimaryKey().getName().equalsIgnoreCase(attrName))	// not a primary key
							{
								hmap.put(vfrag.getName(), Constants.ACCESSED_NOT_PRIMARY);
								verticalFragStateMap.put(tableName, hmap);
							}
						}
						else
						{
							TreeMap<String, Integer> flagMap = new TreeMap<String, Integer>();
							
							for (Fragment frag1 : fragList)
							{
								flagMap.put(frag1.getName(), Constants.ACCESSED_PRIMARY);
							}
							
							if(!vfrag.getPrimaryKey().getName().equalsIgnoreCase(attrName))
							{
								flagMap.put(vfrag.getName(), Constants.ACCESSED_NOT_PRIMARY);
							}
							verticalFragStateMap.put(tableName, flagMap);
						}
					}
				}
			}
		}
	}

	public void checkForVerticalFragment(SelectStatement selectstmt)
	{
		// check in select clause
		SelectAttrList selectAttrList = selectstmt.getAttrList();
		
		for (Attribute attr : selectAttrList.getAttrList())
		{
			String tableName = attr.getTable().getName();
			setVerticalFlagMap(tableName, attr.getName());
		}
		
		// check where clause
		ArrayList<Condition> arrcon = new ArrayList<Condition>();
		ArrayList<Condition> arrconattr = new ArrayList<Condition>();
		parser1.sqltext = selectstmt.toString();
		int ret = parser1.parse();
		if (ret == 0)
		{
			TSelectSqlStatement stmt1 = (TSelectSqlStatement) parser1.sqlstatements.get(0);
			if(stmt1.getWhereClause() != null)
			{
				ModifyWhere w = new ModifyWhere(stmt1.getWhereClause().getCondition(),0,arrcon,arrconattr);
				w.printColumn();
				
				for (int i = 0; i < arrcon.size(); i++) 
				{
					Condition c = arrcon.get(i);
//					System.out.println(c.getLHS()+c.getOperator()+c.getRHS());
					String lhs = c.getLHS();
					String tmp[] = lhs.split("\\.");
					String tableName = tmp[0];
					String attrName = tmp[1];
					setVerticalFlagMap(tableName, attrName);
				}
				
				for (int i = 0; i < arrconattr.size(); i++) 
				{
					
					Condition c = arrconattr.get(i);
					String lhs = c.getLHS();
					String tmp1[] = lhs.split("\\.");
					String tableName1 = tmp1[0];
					String attrName1 = tmp1[1];
					String rhs = c.getRHS();
					String tmp2[] = rhs.split("\\.");
					String tableName2 = tmp2[0];
					String attrName2 = tmp2[1];
					
					if(attrName1.equalsIgnoreCase(attrName2))
					{
						setVerticalFlagMap(tableName1, attrName1);
						setVerticalFlagMap(tableName2, attrName2);
					}
				}
				
			
			}
		}
		
		
	}

	public ArrayList<UpdateStatement> rejectUpdateQueries(UpdateStatement stmt)
	{
		ArrayList<UpdateStatement> arrupdstmt = new ArrayList<UpdateStatement>();
		for (String from : queryEnumerationList)
		{
			UpdateStatement updstmt = new UpdateStatement();

			// check this function call afterwards
			ArrayList<String> fromFragments = populateMaps(from,null);
			FromTableList fromTableList = new FromTableList();
			ArrayList<Table> tablelist = new ArrayList<Table>();

			for (String fName : fromFragments)
			{
					Table table = new Table();
					table.setName(fName);
					// System.out.println(fName);
					tablelist.add(table);
			}
			fromTableList.setList(tablelist);
			updstmt.setTableList(fromTableList);

			SetValueList newSetAttrList = new SetValueList();
			ArrayList<Attribute> oldList = stmt.getSetValueList().getAttrList();
			ArrayList<Attribute> newAttrList = new ArrayList<Attribute>();

			for (Attribute attr : oldList)
			{
				Attribute newAttr = getNewAttribute(attr);
				newAttrList.add(newAttr);
			}
			newSetAttrList.setAttrList(newAttrList);
			newSetAttrList.setValueList(stmt.getSetValueList().getValueList());
			updstmt.setValueList(newSetAttrList);

			parser1.sqltext = stmt.toString();
			int ret = parser1.parse();
			if (ret == 0)
			{
				TUpdateSqlStatement stmt1 = (TUpdateSqlStatement) parser1.sqlstatements
						.get(0);
				if (stmt1.getWhereClause() != null)
				{
					WhereCondition wc = modifyWherePredicate(stmt1);
					updstmt.setWhereCondition(wc);
				}
			}
			FromTableList fromTab = new FromTableList();
			ArrayList<Table> tablelistt = new ArrayList<Table>();

			for (String fName : fromFragments)
			{
					Table table = new Table();
					table.setName(fName);
					tablelistt.add(table);
			}
			fromTab.setList(tablelistt);
			updstmt.setTableList(fromTab);

			System.out.println(updstmt);
			arrupdstmt.add(updstmt);
			localTableFragMap.clear();
		}
		return arrupdstmt;
	}
	
	public ArrayList<SelectStatement> generateSelectFragmentQueries(SelectStatement stmt)
	{
		ArrayList<SelectStatement> arrselstmt=new ArrayList<SelectStatement>();
		
checkForVerticalFragment(stmt);		// check which vertical fragment is present
		
		ArrayList<String> notPresVertFrag = new ArrayList<String>();
		
		for(Map.Entry<String, TreeMap<String,Integer>> set1 : verticalFragStateMap.entrySet())
		{
			TreeMap<String,Integer> fmap = set1.getValue();
			
			int cnt = 0;

			for(Map.Entry<String,Integer> set2:fmap.entrySet())
			{
				if(set2.getValue() == Constants.ACCESSED_PRIMARY)
				{
					notPresVertFrag.add(set2.getKey());
					cnt++;
				}
			}
			
			if(cnt == fmap.size())
			{
				// remove only one vertical fragment 
				notPresVertFrag.remove(0);
			}
		}
		
		for(String from:queryEnumerationList)
		{
			ArrayList<String> fromFragments = populateMaps(from, notPresVertFrag);
			SelectStatement selectstmt = new SelectStatement();
			
			SelectAttrList selectAttrList = stmt.getAttrList();
			SelectAttrList newSelectAttrList = new SelectAttrList();
			ArrayList<Attribute> newAttrList = new ArrayList<Attribute>();
			
			for(Attribute attr : selectAttrList.getAttrList())
			{
				Attribute newAttr = getNewAttribute(attr);
				newAttrList.add(newAttr);
			}
			newSelectAttrList.setAttrList(newAttrList);
			selectstmt.setAttrList(newSelectAttrList);
			
			parser1.sqltext = stmt.toString();
			int ret = parser1.parse();
			if(ret==0)
			{
				TSelectSqlStatement stmt1 = (TSelectSqlStatement)parser1.sqlstatements.get(0);
				if (stmt1.getWhereClause() != null)
				{
					WhereCondition wc = modifyWherePredicate(stmt1);
					selectstmt.setWhereCondition(wc);
				}
				if (stmt1.getGroupByClause() != null)
				{
					String gbstr = stmt1.getGroupByClause().toString().toLowerCase();
					int strt = gbstr.indexOf("group by") + 9;
					String gbstr1 = gbstr.substring(strt).trim().replaceAll("\\s+", " ").trim();
					//System.out.println(gbstr1);

					if (stmt1.getGroupByClause().getHavingClause() != null)
					{
						int end = gbstr1.indexOf("having");
						gbstr1 = gbstr1.substring(0, end).trim();
						//System.out.println(gbstr1);
						HavingCondition hc = Parser.getHavingPredicate(stmt1,2);
						selectstmt.setHavingCondition(hc);
					}
					@SuppressWarnings("unchecked")
					ArrayList<Attribute> attrlist2 = (ArrayList<Attribute>)stmt.getGroupByCondition().getAttrList().clone();
					@SuppressWarnings("unchecked")
					ArrayList<Order> ordlist = (ArrayList<Order>) stmt.getGroupByCondition().getOrder().clone();
					GroupByCondition gbc = new GroupByCondition();
					for(int i=0;i<attrlist2.size();i++)
					{
						Attribute attrb=attrlist2.get(i);
						attrb=getNewAttribute(attrb);
						attrlist2.set(i,attrb);
					}
					gbc.setAttrList(attrlist2);
					gbc.setOrder(ordlist);
					selectstmt.setGroupByCondition(gbc);
/*					ArrayList<Attribute> attrlist2 = new ArrayList<Attribute>();
					ArrayList<Order> ordlist = new ArrayList<Order>();
					GroupByCondition gbc = new GroupByCondition();
					Parser.getOrdPredicate(attrlist2, ordlist, gbstr1);
					for(int i=0;i<attrlist2.size();i++)
					{
						Attribute attrb=attrlist2.get(i);
						attrb=getNewAttribute(attrb);
						attrlist2.set(i,attrb);
					}
					gbc.setAttrList(attrlist2);
					gbc.setOrder(ordlist);
					selectstmt.setGroupByCondition(gbc);
*/				}
/*				if (stmt1.getOrderbyClause() != null)
				{
					System.out.println("*****");
					String gbstr = stmt1.getOrderbyClause().toString().toLowerCase();
					int strt = gbstr.indexOf("order by") + 9;
					String gbstr1 = gbstr.substring(strt).trim().replaceAll("\\s+", " ").trim();
					System.out.println("***"+gbstr1);
					ArrayList<Attribute> attrlist2 = new ArrayList<Attribute>();
					ArrayList<Order> ordlist = new ArrayList<Order>();
					OrderByCondition obc = new OrderByCondition();
					Parser.getOrdPredicate(attrlist2, ordlist, gbstr1);
					for(int i=0;i<attrlist2.size();i++)
					{
						Attribute attrb=attrlist2.get(i);
						attrb=getNewAttribute(attrb);
						attrlist2.set(i,attrb);
					}
					obc.setAttrList(attrlist2);
					obc.setOrder(ordlist);
					selectstmt.setOrderByCondition(obc);
				}
*/				if (stmt1.getOrderbyClause() != null)
				{
					@SuppressWarnings("unchecked")
					ArrayList<Attribute> attrlist2 = (ArrayList<Attribute>)stmt.getOrderByCondition().getAttrList().clone();
					@SuppressWarnings("unchecked")
					ArrayList<Order> ordlist = (ArrayList<Order>)stmt.getOrderByCondition().getOrder().clone();
					OrderByCondition obc = new OrderByCondition();
					for(int i=0;i<attrlist2.size();i++)
					{
						Attribute attrb=attrlist2.get(i);
						attrb=getNewAttribute(attrb);
						attrlist2.set(i,attrb);
					}
					obc.setAttrList(attrlist2);
					obc.setOrder(ordlist);
					selectstmt.setOrderByCondition(obc);
				}

			}
			// set the  Fromlist
			FromTableList fromTableList = new FromTableList();
			ArrayList<Table> tablelist = new ArrayList<Table>();
			
			for(String fName : fromFragments)
			{
					Table table = new Table();
					table.setName(fName);
					tablelist.add(table);
			}
			fromTableList.setList(tablelist);
			selectstmt.setTableList(fromTableList);
			//System.out.println(selectstmt);
			arrselstmt.add(selectstmt);
			localTableFragMap.clear();
		}
		return arrselstmt;
	}
	public static WhereCondition modifyWherePredicate(TCustomSqlStatement querySelect1)
	{
		TExpression te = querySelect1.getWhereClause().getCondition();
		ModifyWhere w = new ModifyWhere(te,2);
		w.printColumn();
		// System.out.println(te.toString());
		WhereCondition wc = new WhereCondition();
		wc.setPredicate(te.toString());
		return wc;
	}

	public Attribute getNewAttribute(Attribute attr)
	{
		Attribute newAttr = new Attribute(attr.getName(), attr.getAlias(), attr.getType(),attr.getFunction(), attr.getTable());
		
		String tableName = newAttr.getTable().getName();
		ArrayList<Fragment> fragList = localTableFragMap.get(tableName);
		Table table = new Table();
		
		for(Fragment frag:fragList)
		{
			if(frag.getType().equalsIgnoreCase(Constants.HOR_FRAG_TYPE) || frag.getType().equalsIgnoreCase(Constants.DER_FRAG_TYPE)||frag.getType().equalsIgnoreCase(Constants.NO_FRAG_TYPE))
			{
				table.setName(frag.getName());
			}
			else if(frag.getType().equalsIgnoreCase(Constants.VER_FRAG_TYPE))
			{
				// check attribute present in which vertical fragment and replace it wisely
				VerticalFragment vfrag = (VerticalFragment)frag;
				ArrayList<Attribute> attrList = vfrag.getAttrList();
				
				for(Attribute attrObj:attrList)
				{
					String fragAttr = attrObj.getName();
					if(fragAttr.equalsIgnoreCase(attr.getName()))
					{
						table.setName(vfrag.getName());
						break;
					}
				}
			}
		}
		newAttr.setTable(table);	// set the fragment Name as table name
		return newAttr;
	}

	public String getNewAttribute(String attr)
	{
		String newAttr;
		//System.out.println(attr);
		String tmp[]=attr.split("\\.");
		String tableName = tmp[0];
		String attrName = tmp[1];
		String fragName="";
		ArrayList<Fragment> fragList = localTableFragMap.get(tableName);
		for(Fragment frag:fragList)
		{
			if(frag.getType().equalsIgnoreCase(Constants.HOR_FRAG_TYPE) || frag.getType().equalsIgnoreCase(Constants.DER_FRAG_TYPE)||frag.getType().equalsIgnoreCase(Constants.NO_FRAG_TYPE))
			{
				fragName=frag.getName();
			}

			else if(frag.getType().equalsIgnoreCase(Constants.VER_FRAG_TYPE))
			{
				// check attribute present in which fragment and replace it wisely
				VerticalFragment vfrag = (VerticalFragment)frag;
				ArrayList<Attribute> attrList = vfrag.getAttrList();
				for(Attribute attrObj:attrList)
				{
					String fragAttr = attrObj.getName();
					if(fragAttr.equalsIgnoreCase(attrName))
					{
						fragName=frag.getName();
						break;
					}
				}
			}
		}
		newAttr=fragName+"."+attrName;
		return newAttr;
	}
	
	public void enumerateAllForFragments(StringBuffer sb,ArrayList<Table> tableList, int c, int n)
	{
		if(c == n)
		{
			sb.delete(sb.length()-1, sb.length());
			queryEnumerationList.add(sb.toString());
			//System.out.println(sb.toString());
			return;
		}
		
		Table table = tableList.get(c);
		
		ArrayList<Fragment> fragmentList = SemanticUtility.tableFragmentListMap.get(table.getName());
		String fragName,fragType;
		
		boolean vflag = true;
		for(Fragment fragment:fragmentList)
		{
			fragName = fragment.getName();
			fragType = fragment.getType();
			if(fragType.equalsIgnoreCase("VR"))
			{
				sb.append(fragName);
				sb.append(JOIN);
			}
			else
			{
				vflag = false;
				StringBuffer sbDup = new StringBuffer();
				sbDup.append(sb);
				sb.append(fragName);
				sb.append(JOIN);
				enumerateAllForFragments(sb,tableList,c+1,n);
				sb.delete(0, sb.length());
				sb.append(sbDup);
			}
		}
		if(vflag)
			enumerateAllForFragments(sb,tableList,c+1,n);
	}
}
