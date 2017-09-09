package com.adb.Executor.SDD1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.adb.Executor.ShellProcess;
import com.adb.fragment.Fragment;
import com.adb.fragment.OptimizeFragmentQuery;
import com.adb.general.AggregateFunction;
import com.adb.general.Attribute;
import com.adb.general.Constants;
import com.adb.general.Order;
import com.adb.general.Site;
import com.adb.select.SelectStatement;
import com.adb.utils.SemanticUtility;

public class SDDImplementor
{
	DatabaseProfile globalDbProfile;
	HashMap<String, Double> relationSizeMap;
	HashMap<String, SelectivityFactorAndSizeAttr> attrDetailMap;
	ArrayList<String> semiJoinSequence = new ArrayList<String>();
	HashMap<String, String> relRelMapping = new HashMap<String, String>();
	HashMap<String, String> bestSemiJoinMap = new HashMap<String, String>();
	HashMap<String, Fragment> interMedRelFragMap = new HashMap<String, Fragment>();
	Site assemblySite;
	ShellProcess sp = new ShellProcess();
	HashMap<String, Integer> attrOccurMap = new HashMap<String, Integer>();
	
	public void executeFragmentQuery(SelectStatement slc, Integer cnt)
	{
		String onSite = assemblySite.getSiteName() + "@" + assemblySite.getIpAddress();
		ArrayList<Attribute> attrList = slc.getAttrList().getAttrList();  
		
		boolean flag = false;
		for(Attribute attr:attrList)
		{
			if(attrOccurMap.containsKey(attr.getName()))
			{
				attrOccurMap.put(attr.getName(), attrOccurMap.get(attr.getName()) + 1);
				flag = true;
			}
			else
				attrOccurMap.put(attr.getName(), 1);
		}
		
		StringBuffer query = new StringBuffer();
		String strQuery = "create table " + Constants.RESULT_SCHEMA_NAME +"."+ Constants.RESULT_TABLE_NAME+cnt +  " as ( " + slc.toString() + ")";
		
		if(cnt == 1)
		{
			if (slc.getGroupByCondition() != null)
			{
				ArrayList<Attribute> attrlist = slc.getGroupByCondition().getAttrList();
				ArrayList<Order> orderlist = slc.getGroupByCondition().getOrder();

				String delim=",";
			    StringBuffer flds = new StringBuffer();
			    
			    for(int i=0;i<attrlist.size();i++)
			    {
			    	Attribute attr = attrlist.get(i);
			    	int tableStrLen = attr.getTable().getName().length();
			    	
			    	if (i>0) 
			    		flds.append(delim);
			    	String tmp=attrlist.get(i).getName();
			    	
			    	if (attrOccurMap.get(tmp) > 1)
			    	{
			    		char endChar = attr.getTable().getName().charAt(tableStrLen - 1);
			    		
						if (Character.isDigit(endChar))
						{
							flds.append(attr.getTable().getName().substring(0, tableStrLen - 1) + "_" + attr.getName());
						}
						else
							flds.append(attr.getTable().getName() + "_" + attr.getName());
			    	}
			    	else
			    		flds.append(tmp);
			        
			        if(orderlist.get(i) == Order.ASCENDING)
			        	flds.append(" ASC");
			        else
			        	flds.append(" DESC");
			    }
				QueryExecutor.FINAL_TABLE_GROUP_LIST = flds.toString();
			}
			
			if(slc.getOrderByCondition()!=null)
			{
				ArrayList<Attribute> attrlist = slc.getOrderByCondition().getAttrList();
				ArrayList<Order> orderlist = slc.getOrderByCondition().getOrder();

				String delim=",";
			    StringBuffer flds = new StringBuffer();
			    
			    for(int i=0;i<attrlist.size();i++)
			    {
			    	Attribute attr = attrlist.get(i);
			    	int tableStrLen = attr.getTable().getName().length();
			    	
			    	if (i>0) 
			    		flds.append(delim);
			    	String tmp=attrlist.get(i).getName();
			    	
			    	if (attrOccurMap.get(tmp) > 1)
			    	{
			    		char endChar = attr.getTable().getName().charAt(tableStrLen - 1);
			    		
						if (Character.isDigit(endChar))
						{
							flds.append(attr.getTable().getName().substring(0, tableStrLen - 1) + "_" + attr.getName());
						}
						else
							flds.append(attr.getTable().getName() + "_" + attr.getName());
			    	}
			    	else
			    		flds.append(tmp);
			        
			        if(orderlist.get(i) == Order.ASCENDING)
			        	flds.append(" ASC");
			        else
			        	flds.append(" DESC");
			    }
			    QueryExecutor.FINAL_TABLE_ORDERBY_LIST = flds.toString();
			}
			
			StringBuffer sb1 = new StringBuffer();
			for(int i=0;i<attrList.size();++i)
			{
				Attribute attr = attrList.get(i);
				String attrType = attr.getType();				
				int tableStrLen = attr.getTable().getName().length();
				
				if(i>0)
					sb1.append(",");
				if(attrOccurMap.get(attr.getName()) > 1)
				{
					char endChar = attr.getTable().getName().charAt(tableStrLen-1);
					if(attr.getFunction() != null)
					{
						sb1.append(attr.getFunction().toUpperCase()+"_Z_");
//						System.out.println("In  "+attr.getFunction());
						if(AggregateFunction.isIntegerType(attr.getFunction()))
							attrType = "integer";
						else if(AggregateFunction.isAvg(attr.getFunction()))
							attrType = "float";
					}
					if(Character.isDigit(endChar))
					{
						sb1.append(attr.getTable().getName().substring(0, tableStrLen-1) + "_" + attr.getName()+" "+ attrType);
					}
					else
						sb1.append(attr.getTable().getName() + "_" + attr.getName()+" "+ attrType);
					
					continue;
				}

				if(attr.getFunction() != null)
				{
					sb1.append(attr.getFunction().toUpperCase()+"_Z_");
//					System.out.println("Innn  "+attr.getFunction());
					if(AggregateFunction.isIntegerType(attr.getFunction()))
						attrType = "integer";
					else if(AggregateFunction.isAvg(attr.getFunction()))
						attrType = "float";
				}
				sb1.append(attr.getName() + " " + attrType);
			}
			System.out.println("sll  :  " + sb1);
			QueryExecutor.FINAL_TABLE_ATTR_LIST=sb1.toString();
		}
		
		if(flag)
		{
//			StringBuffer sb = new StringBuffer();
			StringBuffer sb = new StringBuffer();
			for(int i=0;i<attrList.size();++i)
			{
				Attribute attr = attrList.get(i);
				String attrType = attr.getType();				
				int tableStrLen = attr.getTable().getName().length();
				
				if(i>0)
					sb.append(",");
				if(attrOccurMap.get(attr.getName()) > 1)
				{
					char endChar = attr.getTable().getName().charAt(tableStrLen-1);
					if(attr.getFunction() != null)
					{
						sb.append(attr.getFunction().toUpperCase()+"_Z_");
						System.out.println("Inn"+attr.getFunction());
						if(AggregateFunction.isIntegerType(attr.getFunction()))
							attrType = "integer";
						else if(AggregateFunction.isAvg(attr.getFunction()))
							attrType = "float";
					}
					if(Character.isDigit(endChar))
					{
						sb.append(attr.getTable().getName().substring(0, tableStrLen-1) + "_" + attr.getName()+" "+ attrType);
					}
					else
						sb.append(attr.getTable().getName() + "_" + attr.getName()+" "+ attrType);
					
					continue;
				}
				if(attr.getFunction() != null)
				{
					sb.append(attr.getFunction().toUpperCase()+"_Z_");
					System.out.println("Innn  "+attr.getFunction());
					if(AggregateFunction.isIntegerType(attr.getFunction()))
						attrType = "integer";
					else if(AggregateFunction.isAvg(attr.getFunction()))
						attrType = "float";
				}
				sb.append(attr.getName() + " " + attrType);
			}
				
			/*	Attribute attr = attrList.get(i);
				int tableStrLen = attr.getTable().getName().length();
				
				if(i>0)
					sb.append(",");
				if(attrOccurMap.get(attr.getName()) > 1)
				{
					char endChar = attr.getTable().getName().charAt(tableStrLen-1);
					if(Character.isDigit(endChar))
					{
						sb.append(attr.getTable().getName().substring(0, tableStrLen-1) + "_" + attr.getName()+" "+attr.getType());
					}
					else
						sb.append(attr.getTable().getName() + "_" + attr.getName()+" "+attr.getType());
					continue;
				}
				sb.append(attr.getName() + " " + attr.getType());*/
			
			query.append(" create table " + Constants.RESULT_SCHEMA_NAME +"."+Constants.RESULT_TABLE_NAME+cnt + "(" + sb.toString() + ");\n");
			query.append("insert into "+ Constants.RESULT_SCHEMA_NAME + "." + Constants.RESULT_TABLE_NAME+cnt+" " + slc.toString());
			strQuery = query.toString();
		}
		
		try
		{
			sp.execQuery(Constants.USER_NAME,Constants.PASSWORD,onSite,Constants.TEMP_SCHEMA_NAME,strQuery,Constants.RESULT_SCHEMA_NAME, Constants.RESULT_TABLE_NAME+cnt);
			sp.transferRelation(Constants.USER_NAME, Constants.PASSWORD, onSite, Constants.RESULT_SCHEMA_NAME, Constants.RESULT_TABLE_NAME+cnt , null, Constants.RESULT_SCHEMA_NAME);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		
		attrOccurMap.clear();
	}
	
	void assemblySite()
	{
		Double max = Double.MIN_VALUE;
		String largestRelation = null;

		// find the intermediate relation and move it to the assembly site
		ArrayList<String> intermedList = new ArrayList<String>();
		ArrayList<String> affectedRelList = new ArrayList<String>();

		for (Map.Entry<String, Fragment> interEntry : interMedRelFragMap.entrySet())
		{
			System.out.println(interEntry.getKey());
			intermedList.add(interEntry.getKey());
			String interRel = interEntry.getKey().substring(0, interEntry.getKey().indexOf("_"));
			System.out.println(interEntry.getKey() + " : " + interRel);
			affectedRelList.add(interRel);
		}

		ArrayList<String> notAffectedList = new ArrayList<String>();
		for (Map.Entry<String, Double> entry : relationSizeMap.entrySet())
		{
			if (max < entry.getValue())
			{
				max = entry.getValue();
				largestRelation = entry.getKey();
			}

			// find which relation is not affected
			if (!affectedRelList.contains(entry.getKey()))
			{
				notAffectedList.add(entry.getKey());
			}
		}

		notAffectedList.remove(largestRelation);
		Fragment frag = SemanticUtility.globalFragmentMap.get(largestRelation);
		Site toSite = frag.getSite().get(0);
		assemblySite = toSite;

		System.out.println(toSite.getSiteName());

		try
		{
			// move the notAffected relation to the assembly site
			System.out.println("Not Affected Relation : ");
			for (String notAffected : notAffectedList)
			{
				Site site = SemanticUtility.globalFragmentMap.get(notAffected).getSite().get(0);
				sp.transferRelation(Constants.USER_NAME, Constants.PASSWORD, site.getSiteName() + "@" + site.getIpAddress(),
						Constants.TEMP_SCHEMA_NAME, notAffected, toSite.getSiteName() + "@" + toSite.getIpAddress(), Constants.TEMP_SCHEMA_NAME);
				System.out.println(notAffected);
			}

			// move the affected relation to the assembly site
			System.out.println(" Intermediate Relations : ");
			for (int i = 0; i < intermedList.size(); ++i)
			{
				Site site = interMedRelFragMap.get(intermedList.get(i)).getSite().get(0);
				sp.transferRelation(Constants.USER_NAME, Constants.PASSWORD, site.getSiteName() + "@" + site.getIpAddress(),
						Constants.TEMP_SCHEMA_NAME, intermedList.get(i), toSite.getSiteName() + "@" + toSite.getIpAddress(),
						Constants.TEMP_SCHEMA_NAME);
/*				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				String query1 = br.readLine();
*/				sp.renameRelation(Constants.USER_NAME, Constants.PASSWORD, toSite.getSiteName() + "@" + toSite.getIpAddress(), Constants.TEMP_SCHEMA_NAME, intermedList.get(i), affectedRelList.get(i));
//				System.out.println(intermedList.get(i));
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

	void printMap(HashMap<String, SelectivityFactorAndSizeAttr> atrDMap)
	{
		System.out.println("SF  :  Size");
		for (Map.Entry<String, SelectivityFactorAndSizeAttr> entry : atrDMap.entrySet())
		{
			System.out.println(entry.getKey() + " " + entry.getValue().getSelectivityFactor() + " " + entry.getValue().getSizeAttr());
		}
	}

	void printRelMap(HashMap<String, Double> relationSizeMap)
	{
		System.out.println("Relation   :   Size");
		for (Map.Entry<String, Double> entry : relationSizeMap.entrySet())
		{
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
	}

	public void updateDatabaseProfile(String bestSJ)
	{
		System.out.println("most beneficial sj : " + bestSJ);

		Double SF = attrDetailMap.get(bestSJ).getSelectivityFactor();
		String[] sp = bestSJ.split(Constants.SEMIJOIN_SEP);
		String[] r = sp[0].split("\\.");
		String relToBeReduced = r[0];
		String commonAttr = r[1];
		String[] rem = sp[1].split("\\.");

		String newName = relRelMapping.get(relToBeReduced) + Constants.TRAILER_APPENDED;

		String lhs, rhs;

		lhs = relRelMapping.get(relToBeReduced);
		rhs = relRelMapping.get(rem[0]);

		/*
		 * if(bestSemiJoinMap.containsKey(relRelMapping.get(relToBeReduced))) {
		 * lhs = bestSemiJoinMap.get(relRelMapping.get(relToBeReduced)); } else
		 * { lhs = relRelMapping.get(relToBeReduced); }
		 * if(bestSemiJoinMap.containsKey(relRelMapping.get(rem[0]))) { rhs =
		 * bestSemiJoinMap.get(relRelMapping.get(rem[0])); } else { rhs =
		 * relRelMapping.get(rem[0]); }
		 */

		bestSemiJoinMap.put(newName, lhs + "." + commonAttr + Constants.SEMIJOIN_SEP + rhs + "." + commonAttr);
		relRelMapping.put(relToBeReduced, newName);
		semiJoinSequence.add(newName + Constants.RELATION_EQUALTIY + bestSemiJoinMap.get(newName));

		Double size = relationSizeMap.get(relToBeReduced);
		relationSizeMap.put(relToBeReduced, size * SF); // update the relation
														// size table
		// update the attribute map
		String revSJ = rem[0] + "." + rem[1] + Constants.SEMIJOIN_SEP + relToBeReduced + "." + rem[1];
		double revSJSF = attrDetailMap.get(revSJ).getSelectivityFactor();
		double revSJAttrSize = attrDetailMap.get(revSJ).getSizeAttr();

		attrDetailMap.get(revSJ).setSelectivityFactor(revSJSF * SF);
		attrDetailMap.get(revSJ).setSizeAttr(revSJAttrSize * SF);

		// Now update in the AttrMap where ur R_S.a where S = relToBeReduced
		for (Map.Entry<String, SelectivityFactorAndSizeAttr> entry : attrDetailMap.entrySet())
		{
			@SuppressWarnings("unused")
			String[] sp1 = entry.getKey().split(Constants.SEMIJOIN_SEP);
			String[] rem1 = sp[1].split("\\.");
			if (rem1[0].equalsIgnoreCase(relToBeReduced))
			{
				double attrSize = attrDetailMap.get(revSJ).getSizeAttr();
				attrDetailMap.get(entry.getKey()).setSizeAttr(attrSize * SF);
			}
		}
	}

	public void initiateSDD(DatabaseProfile dbProfile, ArrayList<String> totalList)
	{
		globalDbProfile = dbProfile;
		relationSizeMap = globalDbProfile.getRelationSizeMap();
		attrDetailMap = globalDbProfile.getAttrDetailMap();

		// fill the initial relRelMap
		for (Map.Entry<String, Double> entry : relationSizeMap.entrySet())
		{
			relRelMapping.put(entry.getKey(), entry.getKey());
		}

		int iteration = 0;

		while (true)
		{
			double maxBenefit = 0;
			String bestSJ = null;
			System.out.println(++iteration);

			if (totalList.size() <= 1)
				break;
			for (String pickOne : totalList)
			{
				String[] sp = pickOne.split(Constants.SEMIJOIN_SEP);
				String[] r = sp[0].split("\\.");
				String relToBeReduced = r[0];
				Double size = globalDbProfile.getRelationSizeMap().get(relToBeReduced);
				// System.out.println(pickOne);
				Double cost = globalDbProfile.getAttrDetailMap().get(pickOne).getSizeAttr();
				Double SF = globalDbProfile.getAttrDetailMap().get(pickOne).getSelectivityFactor();
				Double benefit = (1 - SF) * size;

				System.out.println(pickOne + " : cost = " + cost + " benefit = " + benefit);

				if (cost < benefit)
				{
					if ((benefit - cost) > maxBenefit)
					{
						maxBenefit = benefit - cost;
						bestSJ = pickOne;
					}
				}
			}

			// update the database profile accordingly
			if (bestSJ == null)
				break;

			// semiJoinSequence.add(bestSJ);
			updateDatabaseProfile(bestSJ);
			totalList.remove(bestSJ);
		}

		// semi join sequence
		System.out.println("Sequence is = ");

		for (int i=0;i<semiJoinSequence.size();++i)
		{
			String s=semiJoinSequence.get(i);
			System.out.println(s);
			String a[] = s.split(Constants.RELATION_EQUALTIY);
			String intermediateRel = a[0];
			String semijoin = a[1];
			String r[] = semijoin.split(Constants.SEMIJOIN_SEP);
			String fqToRel = r[0];
			String fqFromRel = r[1];
			String toRel = r[0].split("\\.")[0];
			String fromRel = r[1].split("\\.")[0];
			String attr = r[1].split("\\.")[1];
			String toTable = fromRel + "_" + attr;

			// make entry of the intermediate relation into the localfragmentMap
			Fragment f = SemanticUtility.globalFragmentMap.get(toRel);
			Fragment intermRelObj = new Fragment(intermediateRel, f.getType(), f.getSite(), f.getTable());
			interMedRelFragMap.put(intermediateRel, intermRelObj);

			Fragment fromRelObj;
			Fragment toRelObj;
			int indexOf_ = fromRel.indexOf(Constants.TRAILER_APPENDED);

			if (indexOf_ != -1)
			{
				fromRelObj = interMedRelFragMap.get(fromRel);
			}
			else
			{
				fromRelObj = SemanticUtility.globalFragmentMap.get(fromRel);
			}

			indexOf_ = toRel.indexOf(Constants.TRAILER_APPENDED);

			if (indexOf_ != -1)
			{
				toRelObj = interMedRelFragMap.get(toRel);
			}
			else
			{
				toRelObj = SemanticUtility.globalFragmentMap.get(toRel);
			}

			String fromSite = fromRelObj.getSite().get(0).getSiteName() + "@" + fromRelObj.getSite().get(0).getIpAddress();
			String toSite = toRelObj.getSite().get(0).getSiteName() + "@" + toRelObj.getSite().get(0).getIpAddress();

			System.out.println(fromSite);
			System.out.println(toSite);
			try
			{
				sp.getJoinerRelation(Constants.USER_NAME, Constants.PASSWORD, fromSite, Constants.TEMP_SCHEMA_NAME, fromRel, attr,
						Constants.TEMP_SCHEMA_NAME, toTable);

				sp.transferRelation(Constants.USER_NAME, Constants.PASSWORD, fromSite, Constants.TEMP_SCHEMA_NAME, toTable, toSite,
						Constants.TEMP_SCHEMA_NAME);
				String predJoin = r[0] + "=" + toTable + "." + attr;
				// System.out.println(predJoin);
				sp.getReducedFirstTable(Constants.USER_NAME, Constants.PASSWORD, toSite, Constants.TEMP_SCHEMA_NAME, toRel, toTable, predJoin,
						intermediateRel);
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
			
		assemblySite();
		
		
		// System.out.println();
		// printRelMap(relationSizeMap);
		// printMap(attrDetailMap);
	}

	// public void getRelation(String user,String passwd,String fromSite,String
	// fromDB,String fromTable,String toSite,String toDB) throws IOException,
	// InterruptedException
}
