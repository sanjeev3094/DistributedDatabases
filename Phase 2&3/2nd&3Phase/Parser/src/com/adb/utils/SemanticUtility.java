package com.adb.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.adb.Executor.SDD1.QueryExecutor;
import com.adb.fragment.DerivedFragment;
import com.adb.fragment.Fragment;
import com.adb.fragment.HorizontalFragment;
import com.adb.fragment.VerticalFragment;
import com.adb.general.Attribute;
import com.adb.general.Constants;
import com.adb.general.Site;
import com.adb.general.Table;
import com.adb.general.TableType;
import com.adb.select.FromTableList;

public class SemanticUtility {

	public static final String URL = "jdbc:mysql://localhost:3306/syscat" ;
	public static final String USER_NAME = "root" ;
	public static final String PASSWORD = "openstack" ;
	
	public static HashMap<String,HashMap<String,String>> tableDetailsMap = new HashMap<String, HashMap<String,String>>();
	public static HashMap<String,ArrayList<Fragment>> tableFragmentListMap = new HashMap<String, ArrayList<Fragment>>();
	public static HashMap<String,Table> fragmentTableMap = new HashMap<String, Table>(); 
	public static HashMap<String,Fragment> globalFragmentMap = new HashMap<String, Fragment>();
	public static HashMap<String,Fragment> totalFragmentObjMap = new HashMap<String, Fragment>();
	
	public static boolean checkRelation(String relName)
	{
		Connection conn = DatabaseHandler.getConnection(URL,USER_NAME,PASSWORD);
		
		boolean present = false;
		try
		{	 
			PreparedStatement ps = conn.prepareStatement("select * from relation "+"where relName = ?");
			ps.setString(1,relName);
			ResultSet rs = ps.executeQuery();
			if( rs.next())
			{
				present = true ;
			}
		}
		catch(NullPointerException npe)
		{
			System.out.println(" Error : Null pointer exception !!! ");
		}
		catch (SQLException sqle)
		{ 
			System.out.println(" Error : cannot execute the query !!! ");
		}
		finally
		{
			closeConnection(conn);
		}
		
		return present ;
	}
	
	public static void loadTableDetails(FromTableList tablist)
	{
		Connection conn = DatabaseHandler.getConnection(URL, USER_NAME,PASSWORD);
		ArrayList<Table> tableList = tablist.getTableList();
		
		for (Table table:tableList)
		{
			try
			{
				PreparedStatement ps = conn.prepareStatement("select * from relation,relDetail where relation.relId = relDetail.relId and relName = ?");
				ps.setString(1, table.getName());
				ResultSet rs = ps.executeQuery();
				HashMap<String, String> attrTypeMap = new HashMap<String, String>();
				
				while (rs.next())
				{
					String attr = rs.getString("attr");
					String attrType = rs.getString("attrType");
					attrTypeMap.put(attr, attrType);
				}
				
				tableDetailsMap.put(table.getName(), attrTypeMap);
				
			} catch (NullPointerException npe)
			{
				System.out.println(" Error : Null pointer exception !!! ");
			} catch (SQLException sqle)
			{
				System.out.println(" Error : cannot execute the query !!! ");
			} 
		}
	}
	
	public static void closeConnection(Connection conn)
	{
		if( conn != null)
		{
			try 
			{
				conn.close();
			} 
			catch (SQLException e)
			{
					System.out.println(" Error : cannot close the connection  !!!  ");
			}
		}
	}

	public static String[] aggregateFnAndAttr(String attribute)
	{
		String funcAttr[] = new String[2];
		String delims = "[()]";
		String lattr=attribute.toLowerCase();
		if (lattr.startsWith("count") || lattr.startsWith("min")
				|| lattr.startsWith("max") || lattr.startsWith("avg")
				|| lattr.startsWith("sum"))
		{
			funcAttr = attribute.split(delims);
		}
		else
		{
			funcAttr[0] = null;
			funcAttr[1] = attribute;
		}
		return funcAttr;
	}

	public static int checkAttribute(Attribute attrb,String attr, String tableName,String function)
	{
		HashMap<String, String> attrMap;
		if(tableDetailsMap.containsKey(tableName))
		{
			attrMap = tableDetailsMap.get(tableName);
			for (Map.Entry<String, String> attrEntry : attrMap.entrySet())
				if (attrEntry.getKey().equalsIgnoreCase(attr))
				{
					attrb.setName(attrEntry.getKey());
					attrb.setType(attrEntry.getValue());
					if(function!=null)
						attrb.setFunction(function);
					Table tab=new Table();
					tab.setName(tableName);
					tab.setType(TableType.NORMAL);
					attrb.setTable(tab);
					return 0;
				}
			return 1;
		}
		return -1;
	}
	public static int checkAttribute(Attribute attrb,String attr,String function)
	{
		String inTable = null;
		String attrname=null;
		String type=null;
		for (Map.Entry<String, HashMap<String, String>> entry : SemanticUtility.tableDetailsMap.entrySet())
		{
			for (Map.Entry<String, String> attrEntry : entry.getValue().entrySet())
			{
				if (attrEntry.getKey().equalsIgnoreCase(attr))
				{
					if (inTable == null)
					{
						inTable = entry.getKey();
						attrname=attrEntry.getKey();
						type=attrEntry.getValue();
						break;
					} 
					else
						return 1;
				}
			}
		}
		if(inTable!=null)
		{
			Table tab=new Table();
			tab.setName(inTable);
			tab.setType(TableType.NORMAL);
			attrb.setName(attrname);
			attrb.setType(type);
			attrb.setTable(tab);
			if(function!=null)
				attrb.setFunction(function);
			return 0;
		}
		return -1;
	}
	
	public static int getAllAttributeFromTables(ArrayList<Attribute> attrlist,String tbl)
	{
		if(tbl==null)
		{
			for (Map.Entry<String, HashMap<String, String>> tableEntry : SemanticUtility.tableDetailsMap.entrySet())
			{
				String tableName = tableEntry.getKey();
				Table tab=new Table();
				tab.setName(tableName);
				//tab.setAlias(tableName);
				tab.setType(TableType.NORMAL);
				for (Map.Entry<String, String> attrEntry : tableEntry.getValue().entrySet())
				{
					Attribute attr =new Attribute();
					attr.setName(attrEntry.getKey());
					attr.setType(attrEntry.getValue());
					attr.setTable(tab);
					attrlist.add(attr);
				}
			}
		}
		else
		{
			if(!tableDetailsMap.containsKey(tbl))
			{
				return -1;
			}
			else
			{
				Table tab=new Table();
				tab.setName(tbl);
				//tab.setAlias(tableName);
				tab.setType(TableType.NORMAL);
				for (Map.Entry<String, String> attrEntry : tableDetailsMap.get(tbl).entrySet())
				{
					Attribute attr =new Attribute();
					attr.setName(attrEntry.getKey());
					attr.setType(attrEntry.getValue());
					attr.setTable(tab);
					attrlist.add(attr);
				}
	
			}
		}
		return 0;
	}
	
	public static void printTableDetails()
	{
		for(Map.Entry<String,HashMap<String,String>> entry:tableDetailsMap.entrySet())
		{
			System.out.println("table "+ entry.getKey());
			for(Map.Entry<String,String> attrEntry:entry.getValue().entrySet())
			{
				System.out.print("Attr " + attrEntry.getKey());
				System.out.println(" Attr type " + attrEntry.getValue());
			}
		}
	}
	
	public static Site getSiteObj(Integer siteId)
	{
		Connection conn = DatabaseHandler.getConnection(URL, USER_NAME,
				PASSWORD);
		
		Site siteObj = null;
		try
		{
			PreparedStatement ps = conn
					.prepareStatement("select * from site where siteId = ?");
			ps.setInt(1, siteId);
			ResultSet rs = ps.executeQuery();

			if(rs.next())
			{
				String siteName = rs.getString(Constants.SITE_COL_siteName);
				String ipAddress = rs.getString(Constants.SITE_COL_ipAddress);
				String schemaName = rs.getString(Constants.SITE_COL_schemaName);
				String userName = rs.getString(Constants.SITE_COL_userName);
				String passwd = rs.getString(Constants.SITE_COL_passwd);
				siteObj = new Site(siteId, siteName, ipAddress, schemaName, userName, passwd);
			}
		}
		catch (NullPointerException npe)
		{
			System.out.println(" Error : Null pointer exception !!! ");
		}
		catch (SQLException sqle)
		{
			System.out.println(" Error : cannot execute the query !!! ");
		}
		return siteObj;
	}
	
	public static void getAllFragmentsMap(ArrayList<String> fragmentList)
	{
		Connection conn = DatabaseHandler.getConnection(URL, USER_NAME, PASSWORD);

		for (String fragName : fragmentList)
		{
			try
			{
				PreparedStatement ps = conn
						.prepareStatement("select * from relation,fragment,site where relation.relId = fragment.relId and fragment.siteId=site.siteId and fragName = ?");
				ps.setString(1, fragName);
				ResultSet rs = ps.executeQuery();

				while (rs.next())
				{
					String fragType = rs.getString(Constants.FRAG_COL_fragType);
					Integer siteId = Integer.parseInt(rs.getString(Constants.SITE_COL_siteId));
					String siteName = rs.getString(Constants.SITE_COL_siteName);
					String ipAddress = rs.getString(Constants.SITE_COL_ipAddress);
					String schemaName = rs.getString(Constants.SITE_COL_schemaName);
					String userName = rs.getString(Constants.SITE_COL_userName);
					String passwd = rs.getString(Constants.SITE_COL_passwd);
					Site siteObj = new Site(siteId, siteName, ipAddress, schemaName, userName, passwd);
					ArrayList<Site> sites = new ArrayList<Site>();
					sites.add(siteObj);
					
					String relId = rs.getString(Constants.REL_COL_relId);
					String relName = rs.getString(Constants.REL_COL_relName);
					
					QueryExecutor.relationRelIdMap.put(relName, relId);

					Table table = new Table();
					table.setName(relName);
					
					Fragment fragObj = new Fragment();
					fragObj.setName(fragName);
					fragObj.setSite(sites);
					fragObj.setType(fragType);
					fragObj.setTable(table);
					totalFragmentObjMap.put(fragName, fragObj);
				}
			}
			catch (NullPointerException npe)
			{
				System.out.println(" Error : Null pointer exception !!! ");
			}
			catch (SQLException sqle)
			{
				System.out.println(" Error : cannot execute the query !!! ");
			}
		}
	}
	
	
	public static void getFragmentsListForFromTables(ArrayList<Table> tableList)
	{
		Connection conn = DatabaseHandler.getConnection(URL, USER_NAME,PASSWORD);
		
		for (Table tableObj : tableList)
		{
			String table=tableObj.getName();
			try
			{
				PreparedStatement ps = conn.prepareStatement("select * from relation,fragment "+"where relation.relId = fragment.relId and relName = ?");
				ps.setString(1, table);
				ResultSet rs = ps.executeQuery();
				
				TreeMap<String,Fragment> fragMap = new TreeMap<String, Fragment>(); 
				
				while (rs.next())
				{
					String fragName = rs.getString("fragName");
					Integer siteId = Integer.parseInt(rs.getString("siteId"));
					String fragType = rs.getString("fragType");
					
					if(fragMap.containsKey(fragName))
					{
						Fragment f = fragMap.get(fragName);
						if(fragType.equalsIgnoreCase("HR"))
						{
							HorizontalFragment h = (HorizontalFragment)f;
							Site siteObj = getSiteObj(siteId);
							h.getSite().add(siteObj);
							fragMap.put(fragName, h);
							globalFragmentMap.put(fragName, h);
						}
						else if(fragType.equalsIgnoreCase("VR"))
						{
							VerticalFragment v = (VerticalFragment)f;
							Site siteObj = getSiteObj(siteId);
							v.getSite().add(siteObj);
							fragMap.put(fragName, v);
							globalFragmentMap.put(fragName, v);
						}
						else
							globalFragmentMap.put(fragName, f);
						
					}
					else
					{
						ArrayList<Site> siteList = new ArrayList<Site>();
						Site siteObj = getSiteObj(siteId);
						siteList.add(siteObj);
						
						Fragment fragment = new Fragment(fragName,fragType,siteList,tableObj);
						
						if(fragType.equalsIgnoreCase(Constants.HOR_FRAG_TYPE))
						{
							HorizontalFragment hfragObj = getHorizontalFragment(Constants.HFRAG_TABLE, fragment);
//							System.out.print("HR : " + fragment.getName());
//							System.out.println(hfragObj instanceof HorizontalFragment);
							fragMap.put(fragName, hfragObj);
							globalFragmentMap.put(fragName, hfragObj);
							
						}else if(fragType.equalsIgnoreCase(Constants.VER_FRAG_TYPE))
						{
							VerticalFragment vfragObj = getVerticalFragment(Constants.VFRAG_TABLE, fragment);
							fragMap.put(fragName, vfragObj);
							globalFragmentMap.put(fragName, vfragObj);
						}else if(fragType.equalsIgnoreCase(Constants.DER_FRAG_TYPE))
						{
							DerivedFragment dfragObj = getDerivedFragment(Constants.DFRAG_TABLE, fragment);
							fragMap.put(fragName, dfragObj);
							globalFragmentMap.put(fragName, dfragObj);
						}
						else
						{
							fragMap.put(fragName, fragment);
							globalFragmentMap.put(fragName, fragment);
						}
						fragmentTableMap.put(fragName, tableObj);
					}
				}
				
				ArrayList<Fragment> fragmentList = new ArrayList<Fragment>();
				for(Map.Entry<String, Fragment> entry:fragMap.entrySet())
					fragmentList.add(entry.getValue());
				
				tableFragmentListMap.put(table, fragmentList);
			} catch (NullPointerException npe)
			{
				System.out.println(" Error : Null pointer exception !!! ");
			} catch (SQLException sqle)
			{
				System.out.println(" Error : cannot execute the query !!! ");
			} 
		}
	}
	
	public static HorizontalFragment getHorizontalFragment(String fragTable,Fragment fragment)
	{
		Connection conn = DatabaseHandler.getConnection(URL, USER_NAME,PASSWORD);
		String attr = null ,operator = null ,value = null;
		
		try
		{
			PreparedStatement ps = conn.prepareStatement("select * from " + fragTable + " where fragName = ?");
			ps.setString(1, fragment.getName());
			ResultSet rs = ps.executeQuery();	

			// one record comes 
			if (rs.next())
			{
				attr = rs.getString(Constants.HFRAG_COL_attr);
				operator = rs.getString(Constants.HFRAG_COL_operator);
				value = rs.getString(Constants.HFRAG_COL_val); 
			}
		} catch (NullPointerException npe)
		{
			System.out.println(" Error : Null pointer exception !!! ");
		} catch (SQLException sqle)
		{
			System.out.println(" Error : cannot execute the query !!! ");
		} 

		HorizontalFragment hFrag = new HorizontalFragment(fragment.getName(),fragment.getType(), fragment.getSite(), fragment.getTable(),attr, operator, value);

		return hFrag;
		
	}
	
	public static VerticalFragment getVerticalFragment(String fragTable,Fragment fragment)
	{
		Connection conn = DatabaseHandler.getConnection(URL, USER_NAME,PASSWORD);
		ArrayList<Attribute> attrList = new ArrayList<Attribute>();
		Attribute primaryKey = new Attribute();
		
		try
		{
//			PreparedStatement ps = conn.prepareStatement("select * from " + fragTable + " where fragName = ?");
			PreparedStatement ps = conn.prepareStatement("select vFrag.fragName,fragAttr,isPKey from vFrag,fragment,relDetail where vFrag.fragName = fragment.fragName and fragment.relId = relDetail.relId and fragAttr = relDetail.attr and vFrag.fragName =?");
			ps.setString(1, fragment.getName());
			ResultSet rs = ps.executeQuery();	

			// collect all the attributes 
			while (rs.next())
			{
				String attrName = rs.getString(Constants.VFRAG_COL_fragAttr);
				Attribute attrObj = new Attribute();
				attrObj.setName(attrName);
				attrList.add(attrObj);
				Boolean isPkey = rs.getBoolean("isPKey");
				if(isPkey)
					primaryKey.setName(attrName);
			}
		} catch (NullPointerException npe)
		{
			System.out.println(" Error : Null pointer exception !!! ");
		} catch (SQLException sqle)
		{
			System.out.println(" Error : cannot execute the query !!! ");
		} 

		VerticalFragment vFrag = new VerticalFragment(fragment.getName(),fragment.getType(), fragment.getSite(), fragment.getTable(),attrList,primaryKey);
		return vFrag;
	}

	public static DerivedFragment getDerivedFragment(String fragTable,Fragment fragment)
	{
		Connection conn = DatabaseHandler.getConnection(URL, USER_NAME,PASSWORD);
		String joinHFrag = null ,semiJoinAttr = null;
		Fragment newFragment = null;
		
		try
		{
			PreparedStatement ps = conn.prepareStatement("select * from "+ fragTable + " where fragName = ?");
			ps.setString(1, fragment.getName());
			ResultSet rs = ps.executeQuery();

			// one record comes
			if (rs.next())
			{
				joinHFrag = rs.getString(Constants.DFRAG_COL_joinHFrag);
				semiJoinAttr = rs.getString(Constants.DFRAG_COL_semiJoinAttr);
			}

			ps.close();
			// Make HorizontalFragment Object which is joined to it
			
			ps = conn.prepareStatement("select * from relation,fragment where relation.relId = fragment.relId and fragName = ?");
			ps.setString(1, joinHFrag);
			rs = ps.executeQuery();

			String tableName = null;
			Integer siteId;
			ArrayList<Site> siteList = new ArrayList<Site>();

			while (rs.next())
			{
				tableName = rs.getString("relName");
				siteId = Integer.parseInt(rs.getString("siteId"));
				Site siteObj = getSiteObj(siteId);
				siteList.add(siteObj);
			}

			Table tableObj = new Table();
			tableObj.setName(tableName);
			tableObj.setType(TableType.NORMAL);
			newFragment = new Fragment(joinHFrag, Constants.HOR_FRAG_TYPE,siteList, tableObj);

		}
		catch (NullPointerException npe)
		{
			System.out.println(" Error : Null pointer exception !!! ");
		}	
		catch (SQLException sqle)
		{
			System.out.println(" Error : cannot execute the query !!! ");
		}

		HorizontalFragment joinHFragObj = getHorizontalFragment(Constants.HFRAG_TABLE, newFragment);
		globalFragmentMap.put(joinHFrag, joinHFragObj);
		DerivedFragment dFrag = new DerivedFragment(fragment.getName(), fragment.getType(), fragment.getSite(), fragment.getTable(), joinHFragObj, semiJoinAttr);
		
		return dFrag;
	}
	public static String toLocalMapping(String fName)
	{
		Fragment frag = globalFragmentMap.get(fName);
		ArrayList<Site> site = frag.getSite();
		String siteName = site.get(0).getSiteName();
		fName = fName + "@"+ siteName;
		return fName;
	}
	
	public static void printTableFragment()
	{
		for(Map.Entry<String,ArrayList<Fragment>> entry:tableFragmentListMap.entrySet())
		{
			System.out.println("Table : " + entry.getKey());
			ArrayList<Fragment> fragList = entry.getValue();
			System.out.println("    FragName      |      FragType        |      SiteId       | Feature ");
			for(Fragment frag:fragList)
			{
				System.out.print(frag.getName() + "   " + frag.getType() + "   " );
				
				for(Site site:frag.getSite())
					System.out.print(site.getSiteId());
				
				System.out.print("  ");
				
				if(frag instanceof VerticalFragment)
				{
					VerticalFragment vfrag = (VerticalFragment)frag;
					ArrayList<Attribute> alist = vfrag.getAttrList();
					for(Attribute a:alist)
						System.out.print(a.getName()+" ");
					System.out.print("primary key" + " : " + vfrag.getPrimaryKey().getName());
				}
				if(frag instanceof HorizontalFragment)
				{
					HorizontalFragment hfrag = (HorizontalFragment)frag;
					System.out.print(hfrag.getAttr() + hfrag.getOperator() + hfrag.getVal());
				}
				if(frag instanceof DerivedFragment)
				{
					DerivedFragment dfrag = (DerivedFragment)frag;
					System.out.println("");
					System.out.print(dfrag.getJoinHFrag().getName()+ " " + dfrag.getJoinHFrag().getAttr()+ dfrag.getJoinHFrag().getOperator()+dfrag.getJoinHFrag().getVal() + dfrag.getSemiJoinAttr());
				}
				
				System.out.println();
			}
		}
	}

}
