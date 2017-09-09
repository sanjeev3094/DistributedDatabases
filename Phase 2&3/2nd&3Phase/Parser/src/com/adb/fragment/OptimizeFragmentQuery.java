package com.adb.fragment;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.adb.general.Condition;
import com.adb.general.Constants;
import com.adb.general.ModifyWhere;
import com.adb.general.Table;
import com.adb.general.WhereCondition;
import com.adb.select.FromTableList;
import com.adb.select.SelectStatement;
import com.adb.utils.SemanticUtility;

public class OptimizeFragmentQuery
{
	static EDbVendor dbv = EDbVendor.dbvmysql;
	public static HashMap<String,ArrayList<Condition>> queryJoinCond =new HashMap<String, ArrayList<Condition>>();
	public static HashMap<String,ArrayList<Condition>> queryAttributeCond =new HashMap<String, ArrayList<Condition>>();
	public static ArrayList<SelectStatement> optimizeFQ(ArrayList<SelectStatement> arrselstmt) 
	{
		ArrayList<SelectStatement> optimizedList = new ArrayList<SelectStatement>();

		for(SelectStatement selst:arrselstmt)
		{
			// find join condition for vertical fragment
			FromTableList fromTableList = selst.getTableList();
			ArrayList<Table> tables = fromTableList.getTableList();
			HashMap<String,ArrayList<Fragment>> hmap = new HashMap<String, ArrayList<Fragment>>();
			
			for(Table table:tables)
			{
				String fragName = table.getName();
				Fragment fragObj = SemanticUtility.globalFragmentMap.get(fragName);
				if(fragObj.getType().equalsIgnoreCase(Constants.VER_FRAG_TYPE))
				{
					String tab = fragObj.getTable().getName();
					if(hmap.containsKey(tab))
					{
						ArrayList<Fragment> flist = hmap.get(tab);
						flist.add(fragObj);
						hmap.put(tab, flist);
					}
					else
					{
						ArrayList<Fragment> flist = new ArrayList<Fragment>();
						flist.add(fragObj);
						hmap.put(tab, flist);
					}
				}
			}
			
			StringBuffer joinVertCond = new StringBuffer();
			ArrayList<Condition> joincondarr=new ArrayList<Condition>();
			int j = 0;
			for(Map.Entry<String, ArrayList<Fragment>> entry:hmap.entrySet())
			{
				if(j>0)
					joinVertCond.append(" AND ");
				
				ArrayList<Fragment> flist = entry.getValue();
				if(flist.size()>1)
				{
					for(int i=0;i<flist.size()-1;++i)
					{
						if(i>0)
							joinVertCond.append(" AND ");
						VerticalFragment fcurr = (VerticalFragment)flist.get(i);
						VerticalFragment fnext = (VerticalFragment)flist.get(i+1);
						String lhs=fcurr.getName() + "." + fcurr.getPrimaryKey().getName();
						String rhs=fnext.getName() + "." + fnext.getPrimaryKey().getName();
						joinVertCond.append(lhs);
						joinVertCond.append("=");
						joinVertCond.append(rhs);
						joincondarr.add(new Condition(lhs,"=",rhs));
					}
					++j;
				}
			}
			
			//System.out.println("join co : " + joinVertCond.toString());
			//System.out.println(selst.toString());
			boolean whereFound=false;
			TGSqlParser parser2 = new TGSqlParser(dbv);
			parser2.sqltext = selst.toString();
			int ret1 = parser2.parse();
			
			if (ret1 == 0)
			{
				TSelectSqlStatement slcstmt = (TSelectSqlStatement)parser2.sqlstatements.get(0);
				ArrayList<Condition> arrcon = new ArrayList<Condition>();
				ArrayList<Condition> arrconattr = new ArrayList<Condition>();
				if(slcstmt.getWhereClause() != null)
				{
					ModifyWhere w = new ModifyWhere(slcstmt.getWhereClause().getCondition(),0,arrcon,arrconattr);
					w.printColumn();
					
					if(joinVertCond.toString().length()>0)
					{
						for(Condition c:joincondarr)
						{
						 slcstmt.getWhereClause().getCondition().addANDCondition(c.toString());
                         WhereCondition wc1=new WhereCondition();
                         wc1.setPredicate(slcstmt.getWhereClause().getCondition().toString());
                         selst.setWhereCondition(wc1);
                         arrconattr.add(c);
						}

					}
					
					
					optimizedList.add(selst);
					
					queryJoinCond.put(selst.toString(),arrconattr);
					queryAttributeCond.put(selst.toString(),arrcon);
					whereFound=true;
					
					boolean typeFlag = false;
					// eliminate queries on the basis of conditions
					for (int i = 0; i < arrcon.size(); i++) 
					{
						Condition c = arrcon.get(i);
//						System.out.println(c.getLHS()+c.getOperator()+c.getRHS());
						String lhs = c.getLHS();
						String tmp[] = lhs.split("\\.");
						String fragmentName = tmp[0];
						String attrName = tmp[1];
						String rhs = c.getRHS();
						
						Fragment f = SemanticUtility.globalFragmentMap.get(fragmentName);
						//SemanticUtility.printTableFragment();
						
						if (f instanceof HorizontalFragment) 
						{
							HorizontalFragment horFrag = (HorizontalFragment) f;
							if (attrName.equalsIgnoreCase(horFrag.getAttr())) 
							{
								if (!rhs.equalsIgnoreCase(horFrag.getVal())) 
								{
									optimizedList.remove(selst);
									if(queryJoinCond.containsKey(selst.toString()))
										queryJoinCond.remove(selst.toString());
									typeFlag = true;
									break;
								}
							}
						}
					}

					if(typeFlag==false)
					{
						for (int i = 0; i < arrconattr.size(); i++) 
						{
							
							Condition c = arrconattr.get(i);
							String lhs = c.getLHS();
							String tmp1[] = lhs.split("\\.");
							String fragmentName1 = tmp1[0];
							String attrName1 = tmp1[1];
							String rhs = c.getRHS();
							String tmp2[] = rhs.split("\\.");
							String fragmentName2 = tmp2[0];
							String attrName2 = tmp2[1];
							
							
							if(attrName1.equalsIgnoreCase(attrName2))
							{
								Fragment f1 = SemanticUtility.globalFragmentMap.get(fragmentName1);
								Fragment f2 = SemanticUtility.globalFragmentMap.get(fragmentName2);
								if(f1 instanceof HorizontalFragment && f2 instanceof DerivedFragment)
								{
									DerivedFragment derFrag = (DerivedFragment)f2;
									if(!fragmentName1.equalsIgnoreCase(derFrag.getJoinHFrag().getName()))
									{
										optimizedList.remove(selst);
										if(queryJoinCond.containsKey(selst.toString()))
											queryJoinCond.remove(selst.toString());
										break;
									}
								}
								else if(f1 instanceof DerivedFragment && f2 instanceof HorizontalFragment)
								{
									DerivedFragment derFrag = (DerivedFragment)f1;
									if(!fragmentName2.equalsIgnoreCase(derFrag.getJoinHFrag().getName()))
									{
										optimizedList.remove(selst);
										if(queryJoinCond.containsKey(selst.toString()))
											queryJoinCond.remove(selst.toString());
										break;
									}
								}
							}
						}
					}
				}
				else if(slcstmt.getWhereClause()==null && joinVertCond.toString().length()>0)
				{
                     WhereCondition wc1=new WhereCondition();
                     wc1.setPredicate(joinVertCond.toString());
                     selst.setWhereCondition(wc1);
				}
			}
			if(!whereFound)
			{
				optimizedList.add(selst);
				TGSqlParser parser3 = new TGSqlParser(dbv);
				parser3.sqltext = selst.toString();
				int ret2= parser3.parse();
				
				if (ret2 == 0)
				{
					TSelectSqlStatement slcstmt = (TSelectSqlStatement)parser3.sqlstatements.get(0);
					ArrayList<Condition> arrcon = new ArrayList<Condition>();
					ArrayList<Condition> arrconattr = new ArrayList<Condition>();
					if(slcstmt.getWhereClause() != null)
					{
						ModifyWhere w = new ModifyWhere(slcstmt.getWhereClause().getCondition(),0,arrcon,arrconattr);
						w.printColumn();
					}
					queryJoinCond.put(selst.toString(),arrconattr);
				}
				
			}
		}
		
		return optimizedList;
	}
	
}
