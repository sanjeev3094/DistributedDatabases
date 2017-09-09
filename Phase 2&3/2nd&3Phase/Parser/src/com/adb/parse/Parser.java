package com.adb.parse;

/*import java.io.ByteArrayInputStream;
 import java.io.InputStream;

 import org.gibello.zql.ZQuery;
 import org.gibello.zql.ZStatement;
 import org.gibello.zql.ZqlParser;*/
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import gudusoft.gsqlparser.stmt.TUpdateSqlStatement;

import java.util.ArrayList;

import com.adb.fragment.DataLocalisation;
import com.adb.fragment.OptimizeFragmentQuery;
import com.adb.general.AggregateFunction;
import com.adb.general.Attribute;
import com.adb.general.AttributeCheck;
import com.adb.general.ModifyWhere;
import com.adb.general.Order;
import com.adb.general.SetValueList;
import com.adb.general.Table;
import com.adb.general.TableType;
import com.adb.general.WhereCondition;
import com.adb.query.OptimizedQuery;
import com.adb.query.OptimizedSelectQuery;
import com.adb.select.FromTableList;
import com.adb.select.GroupByCondition;
import com.adb.select.HavingCondition;
import com.adb.select.OrderByCondition;
import com.adb.select.SelectAttrList;
import com.adb.select.SelectStatement;
import com.adb.update.UpdateStatement;
import com.adb.utils.SemanticUtility;

public class Parser
{
	static EDbVendor dbv = EDbVendor.dbvmysql;
	static TGSqlParser parser1 = new TGSqlParser(dbv);
	public static SelectAttrList topsellist;
	public static boolean cntflag;
	public static boolean distnctflag;
	public static ArrayList<Attribute> topattrlist;
	
	public static OptimizedQuery parse(String query1)
	{
		parser1.sqltext = query1;
		int ret = parser1.parse();
		if (ret == 0)
		{
			TCustomSqlStatement stmt = parser1.sqlstatements.get(0);

			switch (stmt.sqlstatementtype)
			{
				case sstselect:
					topsellist =new SelectAttrList();
					topattrlist= new ArrayList<Attribute>();
					topsellist.setAttrList(topattrlist);
					cntflag=false;
					distnctflag=false;
					SelectStatement selstmt = analyzeSelectStmt((TSelectSqlStatement) stmt);
					System.out.println("\n\nModified Query");
					System.out.println(selstmt.toString());
					
					ArrayList<SelectStatement> arrselstmt = getInitialFragmentQueries(selstmt);
					
					ArrayList<SelectStatement> optimizedList = OptimizeFragmentQuery.optimizeFQ(arrselstmt);

					System.out.println("\n\nModified Fragment Query");
					for(SelectStatement selst:arrselstmt)
					{
						System.out.println(selst.toString());
					}

					
/*					for(SelectStatement selst:arrselstmt)
					{
						FromTableList ftab=selst.getTableList();
						ArrayList<Table> arrtab=ftab.getTableList();
						for(int i=0;i<arrtab.size();i++)
						{
						//	System.out.println(arrtab.get(i).getName());
							arrtab.get(i).setName(SemanticUtility.toLocalMapping(arrtab.get(i).getName()));
						}
					}
*/					
					System.out.println("\n\nUnoptimised Location Mapping Fragment Query");
					for(SelectStatement selst:arrselstmt)
					{
						System.out.println(selst.toString());
					}					
					System.out.println("\n\nOptimised Location Mapping Fragment Query");
					int j=0;
					OptimizedSelectQuery oq=new OptimizedSelectQuery();
					oq.setCountFlag(cntflag);
					oq.setDistinctFlag(distnctflag);
					oq.setAttrList(optimizedList);
					oq.setTopAttrList(topsellist);
					if(topattrlist.size()>0)
					{
					    System.out.print("SELECT ");
					    System.out.println(topsellist.toString());
					    System.out.println(" FROM ");

					}
					else if(cntflag==true)
					{
					    System.out.print("SELECT ");
					    System.out.println("count(*)");
					    System.out.println(" FROM ");
					}
					
					for(SelectStatement selst:optimizedList)
					{
						if(j>0)
							System.out.println("UNION");
						System.out.println(selst.toString());
						++j;
					}
					//System.out.println(oq.toString());
					return oq;
					
				case sstupdate:
					UpdateStatement updstmt =analyzeUpdateStmt((TUpdateSqlStatement) stmt);
					System.out.println(updstmt.toString());
					System.out.println("\n\nModified Fragment Query");

					ArrayList<UpdateStatement> updstmts=getUpdateFragments(updstmt);
					
					for(UpdateStatement updst:updstmts)
					{
						FromTableList ftab=updst.getTableList();
						ArrayList<Table> arrtab=ftab.getTableList();
						for(int i=0;i<arrtab.size();i++)
						{
							arrtab.get(i).setName(SemanticUtility.toLocalMapping(arrtab.get(i).getName()));
						}
					}
					System.out.println("\n\nLocation Mapping Fragment Query");
					for(UpdateStatement updst:updstmts)
						System.out.println(updst.toString());
					return null;
				case sstdelete:
					return null;
				case sstinsert:
					return null;
			}
		}
		else
		{
			System.out.println("Parser1:" + parser1.getErrormessage());
		}
		return null;

	}	public static ArrayList<SelectStatement> getInitialFragmentQueries(SelectStatement selstmt)
	{
		FromTableList tables = selstmt.getTableList();
		ArrayList<Table> tableList = tables.getTableList();
		SemanticUtility.getFragmentsListForFromTables(tableList);
		//SemanticUtility.printTableFragment();
		
		StringBuffer sb = new StringBuffer();
		DataLocalisation dl = new DataLocalisation();
		dl.enumerateAllForFragments(sb, tableList, 0, tableList.size());
		return dl.generateSelectFragmentQueries(selstmt);
		
	}

	public static ArrayList<UpdateStatement> getUpdateFragments(UpdateStatement updstmt)
	{
		FromTableList tables = updstmt.getTableList();
		ArrayList<Table> tableList = tables.getTableList();
		SemanticUtility.getFragmentsListForFromTables(tableList);
		//SemanticUtility.printTableFragment();
		
		StringBuffer sb = new StringBuffer();
		DataLocalisation dl = new DataLocalisation();
		dl.enumerateAllForFragments(sb, tableList, 0, tableList.size());
		return dl.rejectUpdateQueries(updstmt);
	}
	
	public static UpdateStatement analyzeUpdateStmt(TUpdateSqlStatement queryUpdate)
	{
		UpdateStatement updstmt = new UpdateStatement();
		FromTableList tablelist = getFromTableList(queryUpdate);
		if(tablelist.getTableList().size()>1)
		{
			tablelist.getTableList().remove(0);
		}
		updstmt.setTableList(tablelist);
		SemanticUtility.loadTableDetails(tablelist);
		SetValueList svl=getSetValueList(queryUpdate.getResultColumnList());
		updstmt.setValueList(svl);
		if (queryUpdate.getWhereClause() != null)
		{
			WhereCondition wc = getWherePredicate(queryUpdate);
			updstmt.setWhereCondition(wc);
		}
		return updstmt;

	}
	public static SetValueList getSetValueList(TResultColumnList collist)
	{
		SetValueList svl=new SetValueList();
		ArrayList<Attribute> attrib=new ArrayList<Attribute>();
		ArrayList<String> value=new ArrayList<String>();
		
		for(int i=0;i<collist.size();i++ )
		{
			String trc= collist.getResultColumn(i).toString();
			//System.out.println(trc);
			String att=trc.split("=")[0];
			String val=trc.split("=")[1];
			Attribute attr=AttributeCheck.check(att.trim());
			attrib.add(attr);
			value.add(val.trim());
			//System.out.println(attr.getName()+","+val);
		}
		
		svl.setAttrList(attrib);
		svl.setValueList(value);
		return svl;
	}

	/* Analyze Select Statement */
	public static SelectStatement analyzeSelectStmt(TSelectSqlStatement querySelect)
	{
		if(querySelect.toString().toLowerCase().contains(" distinct "))
			distnctflag=true;
		SelectStatement selstmt = new SelectStatement();

		FromTableList tablelist = getFromTableList(querySelect);
		selstmt.setTableList(tablelist);

		SemanticUtility.loadTableDetails(tablelist);

		SelectAttrList attrlist = getSelectColumnList(querySelect, tablelist);
		selstmt.setAttrList(attrlist);

		if (querySelect.getWhereClause() != null)
		{
			WhereCondition wc = getWherePredicate(querySelect);
			selstmt.setWhereCondition(wc);
		}
		if (querySelect.getGroupByClause() != null)
		{
			String gbstr = querySelect.getGroupByClause().toString().toLowerCase();
			int strt = gbstr.indexOf("group by") + 9;
			String gbstr1 = gbstr.substring(strt).trim().replaceAll("\\s+", " ").trim();

			if (querySelect.getGroupByClause().getHavingClause() != null)
			{
				int end = gbstr1.indexOf("having");
				gbstr1 = gbstr1.substring(0, end).trim();
				HavingCondition hc = getHavingPredicate(querySelect,1);
				selstmt.setHavingCondition(hc);
			}
			ArrayList<Attribute> attrlist2 = new ArrayList<Attribute>();
			ArrayList<Order> ordlist = new ArrayList<Order>();
			GroupByCondition gbc = new GroupByCondition();
			getOrdPredicate(attrlist2, ordlist, gbstr1);
			gbc.setAttrList(attrlist2);
			gbc.setOrder(ordlist);
			selstmt.setGroupByCondition(gbc);
		}
		if (querySelect.getOrderbyClause() != null)
		{
			String gbstr = querySelect.getOrderbyClause().toString().toLowerCase();
			int strt = gbstr.indexOf("order by") + 9;
			String gbstr1 = gbstr.substring(strt).trim().replaceAll("\\s+", " ").trim();
			ArrayList<Attribute> attrlist2 = new ArrayList<Attribute>();
			ArrayList<Order> ordlist = new ArrayList<Order>();
			OrderByCondition obc = new OrderByCondition();
			getOrdPredicate(attrlist2, ordlist, gbstr1);
			obc.setAttrList(attrlist2);
			obc.setOrder(ordlist);
			selstmt.setOrderByCondition(obc);
		}
		//System.out.println(selstmt.toString());

		return selstmt;
	}

	/* Get Select Column List */
	public static SelectAttrList getSelectColumnList(TSelectSqlStatement querySelect1, FromTableList tablelist)
	{
		SelectAttrList attrlist = new SelectAttrList();
		ArrayList<Attribute> list1 = new ArrayList<Attribute>();
		for (int i = 0; i < querySelect1.getResultColumnList().size(); i++)
		{
			String col = querySelect1.getResultColumnList().getResultColumn(i).toString();
			String attrS[] = SemanticUtility.aggregateFnAndAttr(col);
			String function = attrS[0];
			String strattr = attrS[1];
			if (strattr.equalsIgnoreCase("*") && function==null)
				SemanticUtility.getAllAttributeFromTables(list1, null);
			else if(strattr.equalsIgnoreCase("*") && AggregateFunction.isCount(function))
			{
				cntflag=true;
			}
			else if (!strattr.equalsIgnoreCase("*"))
			{
				if (strattr.contains("."))
				{
					Attribute attr = new Attribute();
					String splitAttr[] = strattr.split("\\.");
					String tableName = splitAttr[0];
					String attribute = splitAttr[1];
					// System.out.println("Attr: #"+attribute+"# table: #"+tableName+"#");
					int retval;
					if (attribute.equalsIgnoreCase("*"))
					{
						retval = SemanticUtility.getAllAttributeFromTables(list1, tableName);
						if (retval == -1)
						{
							System.out.println("Unknown Table '" + tableName+"'");
							System.exit(1);
						}
					}
					else
					{
						retval = SemanticUtility.checkAttribute(attr,attribute, tableName, function);
						if (retval != 0)
						{
							if (retval == 1)
								System.out.println("Unknown column '" +attribute+"' in 'field list'");
							else
								if (retval == -1)
									System.out.println("Unknown Table '" + tableName+"'");
							System.exit(1);
						}
						else
						{
							if(function!=null)
							{
								if(AggregateFunction.isAvg(function))
								{
									Attribute attr1=new Attribute(attr.getName(),attr.getAlias(), attr.getType(),function,attr.getTable());
									Attribute attr2=new Attribute(attr.getName(),attr.getAlias(), attr.getType(),"count",attr.getTable());
									Attribute attr3=new Attribute(attr.getName(),attr.getAlias(), attr.getType(),"sum",attr.getTable());
									list1.add(attr2);
									list1.add(attr3);
									topattrlist.add(attr1);
								}
								else
								{
									Attribute attr1=new Attribute(attr.getName(),attr.getAlias(), attr.getType(),function,attr.getTable());
									topattrlist.add(attr1);
								}
							}
							else
							{
								topattrlist.add(attr);
							}
						}
						list1.add(attr);
					}
				}
				else
				{
					Attribute attr = new Attribute();
					int retval = SemanticUtility.checkAttribute(attr, strattr,function);
					if (retval != 0)
					{
						if (retval == 1)
							System.out.println("Column '"+strattr+"' in 'field list' is ambiguous");
						else if (retval == -1)
							System.out.println("Unknown column '" +strattr+"' in 'field list'");
						System.exit(1);
					}
					else
					{
						if(function!=null)
						{
							if(AggregateFunction.isAvg(function))
							{
								Attribute attr1=new Attribute(attr.getName(),attr.getAlias(), attr.getType(),function,attr.getTable());
								Attribute attr2=new Attribute(attr.getName(),attr.getAlias(), attr.getType(),"count",attr.getTable());
								Attribute attr3=new Attribute(attr.getName(),attr.getAlias(), attr.getType(),"sum",attr.getTable());
								list1.add(attr2);
								list1.add(attr3);
								topattrlist.add(attr1);
							}
							else
							{
								Attribute attr1=new Attribute(attr.getName(),attr.getAlias(), attr.getType(),function,attr.getTable());
								topattrlist.add(attr1);
							}
						}
/*						else
							topattrlist.add(attr);
*/					}
					list1.add(attr);
					
				}

			}
		}
		attrlist.setAttrList(list1);
		return attrlist;
	}

	public static FromTableList getFromTableList(TCustomSqlStatement query)
	{
		FromTableList tablelist = new FromTableList();
		ArrayList<Table> list2 = new ArrayList<Table>();
		
		for (int i = 0; i < query.tables.size(); i++)
		{
			String strtab = query.tables.getTable(i).toString();
			if (SemanticUtility.checkRelation(strtab) == false)
			{
				System.out.println("Table " + strtab + "' doesn't exist");
				System.exit(1);
			}
			Table table = new Table();
			table.setName(strtab);
			if (query.tables.getTable(i).getAliasClause() != null)
			{
				System.out.println("{{"+ query.tables.getTable(i).getAliasClause().toString()+ "}}");
				table.setAlias(query.tables.getTable(i).getAliasClause().toString());
			}
			// else
			// table.setAlias(strtab);
			table.setType(TableType.NORMAL);
			list2.add(table);
		}
		tablelist.setList(list2);
		return tablelist;
	}

	public static WhereCondition getWherePredicate(TCustomSqlStatement querySelect1)
	{
		TExpression te = querySelect1.getWhereClause().getCondition();
		ModifyWhere w = new ModifyWhere(te,1);
		w.printColumn();
		// System.out.println(te.toString());
		WhereCondition wc = new WhereCondition();
		wc.setPredicate(te.toString());
		return wc;
	}

	public static void getOrdPredicate(ArrayList<Attribute> attrlist,
			ArrayList<Order> ordlist, String gbstr1)
	{
		String gby[];
		if (gbstr1.contains(","))
			gby = gbstr1.split(",");
		else
		{
			gby = new String[1];
			gby[0] = gbstr1;
		}
		for (String s : gby)
		{
			Attribute gbattr;
			Order ord;
			if (s.trim().toLowerCase().endsWith("desc"))
			{
				String attr = s.split(" ")[0];
				gbattr = AttributeCheck.check(attr);
				ord = Order.DESCENDING;
			}
			else
			{
				if (s.trim().toLowerCase().endsWith("asc"))
				{
					String attr = s.split(" ")[0];
					gbattr = AttributeCheck.check(attr);
				}
				gbattr = AttributeCheck.check(s);
				ord = Order.ASCENDING;
			}
			attrlist.add(gbattr);
			ordlist.add(ord);
		}
	}

	public static HavingCondition getHavingPredicate(TSelectSqlStatement querySelect1,int flag)
	{
		TExpression te = querySelect1.getGroupByClause().getHavingClause();
		ModifyWhere w = new ModifyWhere(te,flag);
		w.printColumn();
		//System.out.println(te.toString());
		HavingCondition hc = new HavingCondition();
		hc.setPredicate(te.toString());
		return hc;
	}
}
