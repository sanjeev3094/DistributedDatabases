package com.adb.Executor.loadDBProfile;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.adb.Executor.SDD1.DatabaseProfile;
import com.adb.Executor.SDD1.FragmentStatistics;
import com.adb.Executor.SDD1.QueryExecutor;
import com.adb.Executor.SDD1.SelectivityFactorAndSizeAttr;
import com.adb.fragment.Fragment;
import com.adb.general.Constants;
import com.adb.general.Site;
import com.adb.utils.DatabaseHandler;
import com.adb.utils.SemanticUtility;

public class ExecuteLoaderUtility
{
	public final String SITE1_NAME = "site1";
	public final String SITE2_NAME = "site2";
	public final String HOME_SITE = "site2";
	
	public static DatabaseProfile dbProfileALL = new DatabaseProfile();
	
	public static void serializeDBProfile()
	{
		try
		{
			FileOutputStream fos = new FileOutputStream("CompleteDBProfile.ser");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(dbProfileALL);
			oos.flush();
			oos.close();
		}
		catch (Exception e)
		{
			System.out.println("Exception during serialization: " + e);
		}
	}
	
	public int findCardOfRsemijoinSOnAttr(String R,String S,String a)
	{
		int card = 0;
		
		Connection conn = DatabaseHandler.getConnection(Constants.URL_WORKSHOP, Constants.USER_NAME, Constants.PASSWORD);

//		System.out.println("select count(*) from " + S + " where " + S+"."+a+ " in (select " + a + " from " + R + ")");
		
		try
		{
			PreparedStatement ps = conn.prepareStatement("select count(*) from " + S + " where " + S+"."+a+ " in (select " + a + " from " + R + ")");
			ResultSet rs = ps.executeQuery();
			if(rs.next())
			{
				card = Integer.parseInt(rs.getString(1));
//				System.out.println("cardinality : " + rs.getString(1));
			}
				
			ps.close();
			rs.close();
		}
		catch (NullPointerException npe)
		{
			System.out.println(" Error : Null pointer exception !!! ");
		}
		catch (SQLException sqle)
		{
			System.out.println(" Error : cannot execute the query !!! ");
		}
		finally
		{
			DatabaseHandler.closeConnection(conn);
		}

		return card;
	}
	
	public void findSFInTwoFragment(FragmentStatistics leftRel,FragmentStatistics rightRel,String commonAttr,HashMap<String, SelectivityFactorAndSizeAttr> atrDMap)
	{
		// SelFactor S.A = card(select count(*) from R where R.a in (select a from S))/card(R)
		// SelFactor R.A = card(select count(*) from S where S.a in (select a from R))/card(S)
		
		int joinCard1 = findCardOfRsemijoinSOnAttr(leftRel.getName(),rightRel.getName(),commonAttr);
		int joinCard2 = findCardOfRsemijoinSOnAttr(rightRel.getName(),leftRel.getName(),commonAttr);
		
		double SFR_A = joinCard1*1.0/rightRel.getNoOfTuples();
		double SFS_A = joinCard2*1.0/leftRel.getNoOfTuples();
		
		int index = leftRel.getAttrList().indexOf(commonAttr);
		double R_A_Size = leftRel.getAttrSizeList().get(index) * leftRel.getNoOfTuples();
		
		index = rightRel.getAttrList().indexOf(commonAttr);
		double S_A_Size  = rightRel.getAttrSizeList().get(index) * rightRel.getNoOfTuples();
		
		SelectivityFactorAndSizeAttr sfAsObj1 = new SelectivityFactorAndSizeAttr();
		sfAsObj1.setSelectivityFactor(SFR_A);
		sfAsObj1.setSizeAttr(R_A_Size);
	
		SelectivityFactorAndSizeAttr sfAsObj2 = new SelectivityFactorAndSizeAttr();
		sfAsObj2.setSelectivityFactor(SFS_A);
		sfAsObj2.setSizeAttr(S_A_Size);
		
		atrDMap.put(rightRel.getName() + "." + commonAttr + Constants.SEMIJOIN_SEP + leftRel.getName() + "." + commonAttr, sfAsObj1);
		atrDMap.put(leftRel.getName() + "." +commonAttr + Constants.SEMIJOIN_SEP + rightRel.getName() + "." + commonAttr, sfAsObj2);
	}
	
	void printMap(HashMap<String, SelectivityFactorAndSizeAttr> atrDMap)
	{
		System.out.println("SF  :  Size");
		for(Map.Entry<String, SelectivityFactorAndSizeAttr> entry:atrDMap.entrySet())
		{
			System.out.println(entry.getKey()+ " " + entry.getValue().getSelectivityFactor() + " " + entry.getValue().getSizeAttr());
		}
	}
	
	void printRelMap(HashMap<String, Double> relationSizeMap)
	{
		System.out.println("Relation   :   Size");
		for(Map.Entry<String,Double> entry:relationSizeMap.entrySet())
		{
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
	}
	
	public void fillSFForCommonAttr(FragmentStatistics left, FragmentStatistics right, HashMap<String, SelectivityFactorAndSizeAttr> attrDeMap)
	{
		ArrayList<String> atts1 = left.getAttrList();

		/*System.out.print(left.getName()+": ");
		for(String att:atts1)
			System.out.print(att+", ");
		
		System.out.println();
		System.out.print(right.getName()+": ");
		ArrayList<String> atts21 = right.getAttrList();
		for(String att:atts21)
			System.out.print(att+", ");
		System.out.println();*/
		// find common attribute

		String commonAttr = null;

		for (String att1 : atts1)
		{
			ArrayList<String> atts2 = right.getAttrList();

			for (String att2 : atts2)
			{
				if (att1.equalsIgnoreCase(att2))
				{
					commonAttr = att1;
					findSFInTwoFragment(left,right,commonAttr,attrDeMap);
				}
			}
		}
	}
	
	
	public String[][] getColumnSizeList(String fragName, String fragType, String relId)
	{
		int cnt = 0;
		String[][] list = new String[0][0];
		Connection conn1 = DatabaseHandler.getConnection(Constants.URL_SYSCAT, Constants.USER_NAME, Constants.PASSWORD);
		
		try
		{
			if (fragType.compareTo("HR") == 0 || fragType.compareTo("DR") == 0 || fragType.compareTo("NF") == 0)
			{
//				System.out.println(fragName);
				String s = "select * from relDetail where relId=" + relId + ";";
//				System.out.println(s);
				PreparedStatement ps1 = conn1.prepareStatement(s);
				ResultSet rs1 = ps1.executeQuery();

				rs1.last();
				int size = rs1.getRow();
				list = new String[size][2];
				rs1.first();
				do
				{
					String typ = rs1.getString(3);
//					System.out.println("Col: "+rs1.getString(2));
					list[cnt][0] = rs1.getString(2);
					list[cnt++][1] = "" + getColumnSize(typ);
				}
				while (rs1.next());
				ps1.close();
				rs1.close();
			}
			else if (fragType.compareTo("VR") == 0)
			{
				String s = "select fragAttr,attrType from vFrag,relDetail where fragAttr=attr and relId=" + relId + " and fragName='"
						+ fragName + "';";
//				System.out.println(s);
				PreparedStatement ps1 = conn1.prepareStatement(s);
				ResultSet rs1 = ps1.executeQuery();
				rs1.last();
				int size = rs1.getRow();
				list = new String[size][2];
				rs1.first();
				do
				{
					String typ = rs1.getString(2);
					list[cnt][0] = rs1.getString(1);
					list[cnt++][1] = "" + getColumnSize(typ);
				}while (rs1.next());
				ps1.close();
				rs1.close();
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
		finally
		{
			DatabaseHandler.closeConnection(conn1);
		}
		return list;
	}

	public int getRowCount(String fragment)
	{
		Connection conn = DatabaseHandler.getConnection(Constants.URL_INFO_SCHEMA, Constants.USER_NAME, Constants.PASSWORD);
		int rowCnt = 0;

		try
		{
			PreparedStatement ps = conn.prepareStatement("select cardinality from statistics where table_name = ? ");
			ps.setString(1, fragment);
			ResultSet rs = ps.executeQuery();
			while (rs.next())
			{
				if (rs.getString(1) != null)
				{
					rowCnt = Integer.parseInt(rs.getString(1));
					break;
				}
			}
			ps.close();
			rs.close();
		}
		catch (NullPointerException npe)
		{
			System.out.println(" Error : Null pointer exception !!! ");
		}
		catch (SQLException sqle)
		{
			System.out.println(" Error : cannot execute the query !!! ");
		}
		finally
		{
			DatabaseHandler.closeConnection(conn);
		}
		return rowCnt;
	}

	public int getRowCountForStat(String fragment)
	{
		Connection conn = DatabaseHandler.getConnection(Constants.URL_WORKSHOP, Constants.USER_NAME, Constants.PASSWORD);
		int rowCnt = 0;

		try
		{
			PreparedStatement ps = conn.prepareStatement("select count(*) from " + fragment);
			ResultSet rs = ps.executeQuery();

			if(rs.next())
			{
					rowCnt = Integer.parseInt(rs.getString(1));
			}
			ps.close();
			rs.close();
		}
		catch (NullPointerException npe)
		{
			System.out.println(" Error : Null pointer exception !!! ");
		}
		catch (SQLException sqle)
		{
			System.out.println(" Error : cannot execute the query !!! ");
		}
		finally
		{
			DatabaseHandler.closeConnection(conn);
		}
		return rowCnt;
	}

	public int getRowSize(String fragName, String fragType, String relId)
	{
		Connection conn1 = DatabaseHandler.getConnection(Constants.URL_SYSCAT, Constants.USER_NAME, Constants.PASSWORD);
		int sizeRow = 0;
		try
		{
			if (fragType.compareTo("HR") == 0 || fragType.compareTo("DR") == 0 || fragType.compareTo("NF") == 0)
			{
//				System.out.println(fragName);
				String s = "select * from relDetail where relId=" + relId + ";";
//				System.out.println(s);
				PreparedStatement ps1 = conn1.prepareStatement(s);
				ResultSet rs1 = ps1.executeQuery();
				while (rs1.next())
				{
					String typ = rs1.getString(3);
//					System.out.println(rs1.getString(2) + " " + typ);
					sizeRow += getColumnSize(typ);
				}
				ps1.close();
				rs1.close();
			}
			else if (fragType.compareTo("VR") == 0)
			{
				String s = "select fragAttr,attrType from vFrag,relDetail where fragAttr=attr and relId=" + relId + " and fragName='"
						+ fragName + "';";
//				System.out.println(s);
				PreparedStatement ps1 = conn1.prepareStatement(s);
				ResultSet rs1 = ps1.executeQuery();
				while (rs1.next())
				{
					String typ = rs1.getString(2);
//					System.out.println(rs1.getString(1) + " " + typ);
					sizeRow += getColumnSize(typ);
				}
				ps1.close();
				rs1.close();
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
		finally
		{
			DatabaseHandler.closeConnection(conn1);
		}
		return sizeRow;
	}

	public int getColumnSize(String col)
	{
		int attributeSize = 0;
		if (col.contains("varchar"))
			attributeSize = Integer.parseInt(col.substring(8, col.length() - 1));
		else if (col.compareTo("date") == 0 || col.compareTo("time") == 0)
			attributeSize = 3;
		else if (col.compareTo("datetime") == 0)
			attributeSize = 8;
		else if (col.compareTo("float") == 0 || col.contains("int"))
			attributeSize = 4;
		else
			return -1;
		return attributeSize;
	}

	public ArrayList<String> loadFragmentListFromDB()
	{
		Connection conn = DatabaseHandler.getConnection(Constants.URL_SYSCAT, Constants.USER_NAME, Constants.PASSWORD);
		ArrayList<String> fragmentList = new ArrayList<String>();

		try
		{
			PreparedStatement ps = conn.prepareStatement("select * from fragment");
			ResultSet rs = ps.executeQuery();

			while (rs.next())
			{
				String fragName = rs.getString(Constants.FRAG_COL_fragName);
				fragmentList.add(fragName);
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
		finally
		{
			DatabaseHandler.closeConnection(conn);
		}
		return fragmentList;
	}

	ArrayList<FragmentStatistics> findFragmentStatistics(ArrayList<String> fragments)
	{
		ArrayList<FragmentStatistics> fragStatList = new ArrayList<FragmentStatistics>();
		
		for (String frag : fragments)
		{
			Fragment fragObj = SemanticUtility.totalFragmentObjMap.get(frag);
		
			int rowSize = getRowSize(frag, fragObj.getType(), QueryExecutor.relationRelIdMap.get(fragObj.getTable().getName()));
			int cardinality = getRowCountForStat(frag);
		
			String[][] list = getColumnSizeList(frag, fragObj.getType(), QueryExecutor.relationRelIdMap.get(fragObj.getTable().getName()));

			ArrayList<String> attrList = new ArrayList<String>();
			ArrayList<Integer> attrSizeList = new ArrayList<Integer>();
			
			for (String[] str : list)
			{
				attrList.add(str[0]);
				attrSizeList.add(Integer.parseInt(str[1]));
//				System.out.println(str[0] + " " + str[1]);
			}
			
			FragmentStatistics fragStatObj = new FragmentStatistics();
			fragStatObj.setName(frag);
			fragStatObj.setNoOfTuples(cardinality);
			fragStatObj.setSizeOfTuple(rowSize);
			fragStatObj.setAttrList(attrList);
			fragStatObj.setAttrSizeList(attrSizeList);
			fragStatList.add(fragStatObj);
		}
		return fragStatList;
	}

	void calculateSelectivityFactor(ArrayList<FragmentStatistics> fragStatList, HashMap<String, SelectivityFactorAndSizeAttr> attrDetMap)
	{
//		System.out.println(fragStatList.size());
		
		for(int i=0;i<fragStatList.size()-1;++i)
		{
//			FragmentStatistics fragStat = fragStatList.get(i);
//			System.out.println(fragStat.getName() + " " + fragStat.getNoOfTuples()+ " " + fragStat.getSizeOfTuple());
			for(int j=i+1;j<fragStatList.size();++j)
			{
				if(fragStatList.get(i).getName().startsWith(Constants.STUDENT_PREFIX) && fragStatList.get(j).getName().startsWith(Constants.STUDENT_PREFIX))
				{
					continue;
				}
				else if(fragStatList.get(i).getName().startsWith(Constants.ENROLLMENT_PREFIX) && fragStatList.get(j).getName().startsWith(Constants.ENROLLMENT_PREFIX))
				{
					continue;
				}
				else if(fragStatList.get(i).getName().startsWith(Constants.COURSE_PREFIX) && fragStatList.get(j).getName().startsWith(Constants.COURSE_PREFIX) && fragStatList.get(i).getName().length()<=8 && fragStatList.get(j).getName().length()<=8)
				{
					continue;
				}
//				System.out.println(fragStat.getName() + " , " + fragStatList.get(j).getName());
				fillSFForCommonAttr(fragStatList.get(i), fragStatList.get(j), attrDetMap);
			}
		}
	}

	public void calculateDatabaseProfile(ArrayList<String> fragments)
	{
		// find fragment statistics of each fragments
		ArrayList<FragmentStatistics> fragStatList = findFragmentStatistics(fragments);

		// fill the profile for relationSize
		HashMap<String, Double> relationSizeMap = new HashMap<String, Double>();

		for (FragmentStatistics fragStat : fragStatList)
		{
			Double relSize = 1.0 * fragStat.getNoOfTuples() * fragStat.getSizeOfTuple();
			relationSizeMap.put(fragStat.getName(), relSize);
		}

		dbProfileALL.setRelationSizeMap(relationSizeMap);

		HashMap<String, SelectivityFactorAndSizeAttr> attrDMap = new HashMap<String, SelectivityFactorAndSizeAttr>();
		// fill the database profile for relationSize
		calculateSelectivityFactor(fragStatList,attrDMap);
		dbProfileALL.setAttrDetailMap(attrDMap);
		
		printRelMap(relationSizeMap);
		printMap(attrDMap);
	}

	public void fetchHomeSiteDetails(ArrayList<String> fragments)
	{

	}

	public void fetchFromOtherTwoSites(ArrayList<String> fragments1, ArrayList<String> fragments2)
	{

	}

	public void loadRelationSize(ArrayList<String> fragments)
	{
		ArrayList<Site> sites = new ArrayList<Site>(3);
		ArrayList<String> site1List = new ArrayList<String>(8);
		ArrayList<String> site2List = new ArrayList<String>(8);
		ArrayList<String> site3List = new ArrayList<String>(8);

		for (String frag : fragments)
		{
			Fragment fragObj = SemanticUtility.totalFragmentObjMap.get(frag);
			ArrayList<Site> sitesList = fragObj.getSite();

//			System.out.println(fragObj.getName());

			for (Site site : sitesList)
			{
				String siteName = site.getSiteName();

				if (sites.size() != 0)
				{
					boolean add = true;
					for (Site s : sites)
						if (s.getSiteName().equalsIgnoreCase(siteName))
						{
							add = false;
							break;
						}
					if (add)
						sites.add(site);
				}
				else
					sites.add(site);

				if (siteName.equalsIgnoreCase(SITE1_NAME))
				{
					site1List.add(frag);
				}
				else if (siteName.equalsIgnoreCase(SITE2_NAME))
				{
					site2List.add(frag);
				}
				else
				{
					site3List.add(frag);
				}
			}
		}

		System.out.println(" List of fragments : ");
		for (String frag : site2List)
			System.out.println(frag);
		System.out.println(" List of sites : ");
		for (Site site : sites)
			System.out.println(site.getSiteName());

		// home site is site2 fetch details from here
		fetchHomeSiteDetails(site2List);

		// else call to server and fetch details
		fetchFromOtherTwoSites(site1List, site3List);

	}

}
