import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.adb.general.Constants;
import com.adb.utils.DatabaseHandler;
import com.adb.utils.SemanticUtility;

public class Size
{
	public static final String URL = "jdbc:mysql://localhost:3306/syscat";
	public static final String URL1 = "jdbc:mysql://localhost:3306/college";
	public static final String URL2 = "jdbc:mysql://localhost:3306/information_schema";

	public static String getRelId(String relName)
	{
		Connection conn = DatabaseHandler.getConnection(URL, SemanticUtility.USER_NAME, SemanticUtility.PASSWORD);
		String relId = "";
		try
		{
			PreparedStatement ps = conn
					.prepareStatement("select * from relation,relDetail where relation.relId=relDetail.relId and relName='" + relName
							+ "';");
			ResultSet rs = ps.executeQuery();

			if (rs.next())
			{
				relId = rs.getString(1);
				System.out.println(relId);
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
			closeConnection(conn);
		}
		return relId;
	}

	public static String[][] getFragments(String relId, int[] siz)
	{
		Connection conn = DatabaseHandler.getConnection(URL, SemanticUtility.USER_NAME, SemanticUtility.PASSWORD);
		String[][] frags = new String[10][2];
		int size = 0;
		try
		{
			PreparedStatement ps = conn.prepareStatement("select * from fragment where relId=" + relId + ";");
			ResultSet rs = ps.executeQuery();
			frags = new String[10][2];
			size = 0;
			while (rs.next())
			{
				frags[size][0] = rs.getString(1);
				frags[size][1] = rs.getString(4);
				System.out.println(frags[size][0] + " " + frags[size][1]);
				++size;
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
			closeConnection(conn);
		}

		String[][] fragments = new String[size][2];
		for (int i = 0; i < size; i++)
		{
			fragments[i][0] = frags[i][0];
			fragments[i][1] = frags[i][1];
		}
		siz[0] = size;
		return fragments;
	}

	public static int getRowSize(String fragName, String fragType, String relId)
	{
		Connection conn1 = DatabaseHandler.getConnection(Constants.URL_SYSCAT, Constants.USER_NAME, Constants.PASSWORD);
		int sizeRow = 0;
		try
		{
			if (fragType.compareTo("HR") == 0 || fragType.compareTo("DR") == 0 || fragType.compareTo("NF") == 0)
			{
				System.out.println(fragName);
				String s = "select * from relDetail where relId=" + relId + ";";
				System.out.println(s);
				PreparedStatement ps1 = conn1.prepareStatement(s);
				ResultSet rs1 = ps1.executeQuery();
				while (rs1.next())
				{
					String typ = rs1.getString(3);
					System.out.println(rs1.getString(2) + " " + typ);
					sizeRow += getColumnSize(typ);
				}
				ps1.close();
				rs1.close();
			}
			else if (fragType.compareTo("VR") == 0)
			{
				String s = "select fragAttr,attrType from vFrag,relDetail where fragAttr=attr and relId=" + relId + " and fragName='"
						+ fragName + "';";
				System.out.println(s);
				PreparedStatement ps1 = conn1.prepareStatement(s);
				ResultSet rs1 = ps1.executeQuery();
				while (rs1.next())
				{
					String typ = rs1.getString(2);
					System.out.println(rs1.getString(1) + " " + typ);
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
			closeConnection(conn1);
		}
		return sizeRow;
	}

	public static String[][] getSizeList(String fragName, String fragType, String relId)
	{
		int cnt = 0;
		String[][] list = new String[0][0];
		Connection conn1 = DatabaseHandler.getConnection(URL, SemanticUtility.USER_NAME, SemanticUtility.PASSWORD);
		try
		{
			if (fragType.compareTo("HR") == 0 || fragType.compareTo("DR") == 0 || fragType.compareTo("NF") == 0)
			{
				System.out.println(fragName);
				String s = "select * from relDetail where relId=" + relId + ";";
				System.out.println(s);
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
				System.out.println(s);
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
				}
				while (rs1.next());
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
			closeConnection(conn1);
		}
		return list;
	}

	public static int getRowCount(String query)
	{
		System.out.println(query);
		Connection conn = DatabaseHandler.getConnection(URL2, SemanticUtility.USER_NAME, SemanticUtility.PASSWORD);
		int rowCnt = 0;

		try
		{
			PreparedStatement ps = conn.prepareStatement(query);
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
			closeConnection(conn);
		}
		return rowCnt;
	}

	public static int getColCount(String query)
	{
		System.out.println(query);
		Connection conn = DatabaseHandler.getConnection(URL1, SemanticUtility.USER_NAME, SemanticUtility.PASSWORD);
		int colCnt = 0;
		try
		{
			PreparedStatement ps = conn.prepareStatement(query);
			ResultSet rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			colCnt = rsmd.getColumnCount();
			ps.close();
			rs.close();
		}
		catch (NullPointerException npe)
		{
			System.out.println("getColCount:Error : Null pointer exception !!! ");
		}
		catch (SQLException sqle)
		{
			System.out.println("getColCount:Error : cannot execute the query !!! ");
		}
		finally
		{
			closeConnection(conn);
		}
		return colCnt;
	}

	public static void tmp()
	{
		// String relName="student";
		String relName = "student";
		// String relName="department";
		// String relName="enrollment";
		String relId = getRelId(relName);
		int[] tmp = { 0 };
		String[][] fragments = getFragments(relId, tmp);
		int fr = tmp[0];
		System.out.println(fr);
		for (int i = 0; i < fr; i++)
			getRowSize(fragments[i][0], fragments[i][1], relId);
	}

	public static void main(String[] args)
	{
		// tmp();
		int sizeRow = getRowSize("faculty2", "VR", "2");
		System.out.println("Printing size of row of table faculty2 ");
		System.out.println("Size= " + sizeRow);

		String[][] list = getSizeList("student2", "HR", "1");
		System.out.println("printing sizeList of student2 ");
		for (String[] str : list)
			System.out.println(str[0] + " " + str[1]);

		int colcnt = getColCount("select * from student2;");
		System.out.println("Column count= " + colcnt);

		int rowcnt = getRowCount("select cardinality from statistics where table_name='enrollment2';");
		System.out.println("Row count= " + rowcnt);
	}

	public static int getColumnSize(String col)
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

	public static void closeConnection(Connection conn)
	{
		if (conn != null)
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

}
