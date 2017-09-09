import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.adb.Executor.ShellProcess;
import com.adb.Executor.SDD1.QueryExecutor;
import com.adb.Executor.loadDBProfile.LoadDBProfile;
import com.adb.fragment.DataLocalisation;
import com.adb.fragment.OptimizeFragmentQuery;
import com.adb.general.Constants;
import com.adb.parse.Parser;
import com.adb.query.OptimizedQuery;
import com.adb.query.OptimizedSelectQuery;
import com.adb.query.RemoveCartesianProduct;
import com.adb.select.SelectStatement;
import com.adb.utils.DatabaseHandler;
import com.adb.utils.SemanticUtility;

public class MainApp
{
	String finalTable;
	
	public static void main(String args[])
	{
		LoadDBProfile loadProfile = new LoadDBProfile();
		loadProfile.printMap();
		loadProfile.printRelMap();
		
		String query1;
		// Parser parser=new Parser();
		// select * from department;
		// select hometown, salary from faculty where emailId='100';
		// select studName,department.* from student,department where student.deptId=department.deptId and prog='BTech';
		// SELECT product_id, product_name, product_price FROM products p1 WHERE product_price < 100 and q=0 or t=0;
		// select * from student where aa in (1,2) and aaa in (select * from aa) and prog = 'Btech' and deptId = 'CS' or yoa = 2010 or name like 'a%s*';
		// select * from student where prog = 'Btech' and deptId = 'CS' or yoa = '2010';
		// select sum(faculty.deptId),sum(rollNo),faculty.* from student,faculty where faculty.deptId=studName and prog = 'Btech' and student.deptId = 'CS' or yoa = '2010' group by prog desc,prog desc having student.deptId='CS';
		// update student set student.deptId='DDD',student.studName='999' where student.deptId='Cc' and student.studName='Pawan';
		// select courseName,studName from student,enrollment,courseOffering,course where student.rollNo = enrollment.rollNo and enrollment.oId = courseOffering.oId and courseOffering.courseId = course.courseId and cType='elective' and prog = 'BTech';
		//select * from student,courseOffering,enrollment where student.rollNo=enrollment.rollNo and prog='BTech' and enrollment.oId=courseOffering.oId;
		// select count(*) from student;
		// update student set student.deptId='DDD',student.studName='999' where student.deptId='Cc';
		// update faculty set deptId='123',salary=10000 where deptId='111';
		// select sum(faculty.deptId),sum(rollNo),faculty.*,courseOffering.* from student,faculty,courseOffering where faculty.deptId=studName and prog = 'Btech' and student.deptId = 'CS' or yoa = '2010' group by prog desc,prog desc having student.deptId='CS';
		// select * from student,enrollment,courseOffering,faculty,department where student.rollNo=enrollment.rollNo and enrollment.oId=courseOffering.oId and faculty.facultyId=courseOffering.facultyId and faculty.deptId=department.deptId and prog='BTech';
		// select * from enrollment;
		// select * from faculty;
		// select * from courseOffering";
		// select * from department";
		// select * from student,enrollment where student.rollNo = enrollment.rollNo and prog='BTech';
		// select facultyName,min(salary) from faculty,department where faculty.deptId=department.deptId and deptName='Computer Science';
		// select * from student order by rollNo;
		// select courseName from course,courseOffering where course.courseId=courseOffering.courseId and cType='system' and semester='Spring' and oYear=2012; 
		// update course set credit=credit+7;
		//select studName,department.* from student,department where student.deptId=department.deptId and prog='BTech' group by department.deptId;
		//select courseName,studName,student.rollNo from student,enrollment,courseOffering,course where student.rollNo = enrollment.rollNo and enrollment.oId = courseOffering.oId and courseOffering.courseId = course.courseId and cType='elective' and prog = 'BTech' order by student.rollNo desc;
		while (true)
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			try
			{
				/*
				 * SDDImpl sdd = new SDDImpl(); sdd.fillDetailsForSDD(slc);
				 */

				System.out.println("mysql> ");
				query1 = br.readLine();
				if (query1.toLowerCase().startsWith("quit"))
				{
					System.out.println("bye");
					break;
				}
				// System.out.println(query1);
				long t = System.currentTimeMillis();
				// parser.parse(query1);
				OptimizedQuery oq = Parser.parse(query1);
				System.out.println("\nResponse Time :" + (System.currentTimeMillis() - t) + " ms.\n");
				// String q="SELECT course3.courseName,student1.studName FROM student1,enrollment1,courseOffering1,course3 WHERE student1.rollNo = enrollment1.rollNo and enrollment1.oId = courseOffering1.oId and courseOffering1.courseId = course3.courseId and course3.cType='elective' and student1.prog = 'BTech'";
				// EDbVendor dbv = EDbVendor.dbvmysql;
				// TGSqlParser parser2 = new TGSqlParser(dbv);
				// parser2.sqltext = q;
				// int ret1 = parser2.parse();
				// ArrayList<Condition> arrcon = new ArrayList<Condition>();
				t = System.currentTimeMillis();
				if (oq instanceof OptimizedSelectQuery)
				{
					OptimizedSelectQuery oq1 = (OptimizedSelectQuery) oq;
					
					ShellProcess sp = new ShellProcess();
					
					int cnt = 1;
					try
					{
						sp.emptyDB(Constants.USER_NAME, Constants.PASSWORD, Constants.site1, Constants.RESULT_SCHEMA_NAME);
						sp.emptyDB(Constants.USER_NAME, Constants.PASSWORD, Constants.site2, Constants.RESULT_SCHEMA_NAME);
						sp.emptyDB(Constants.USER_NAME, Constants.PASSWORD, Constants.site3, Constants.RESULT_SCHEMA_NAME);
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
					}
					
					// intitialising orderby list
					QueryExecutor.FINAL_TABLE_ORDERBY_LIST = null;
					
					for (SelectStatement slc : oq1.getAttrList())
					{
						RemoveCartesianProduct.joinMap.clear();
						RemoveCartesianProduct.removeCP(slc);

						try
						{
							sp.emptyDB(Constants.USER_NAME, Constants.PASSWORD, Constants.site1, Constants.TEMP_SCHEMA_NAME);
							sp.emptyDB(Constants.USER_NAME, Constants.PASSWORD, Constants.site2, Constants.TEMP_SCHEMA_NAME);
							sp.emptyDB(Constants.USER_NAME, Constants.PASSWORD, Constants.site3, Constants.TEMP_SCHEMA_NAME);
						}
						catch (InterruptedException e)
						{
							e.printStackTrace();
						}
						// make a query plan
						QueryExecutor qe = new QueryExecutor();
						qe.populateTablesofSDD(slc, cnt);
						cnt++;
					}

					QueryExecutor qe = new QueryExecutor();
					
					String groupBy = "";
					if(oq1.getAttrList().get(0).getGroupByCondition() != null)
					{
//						groupBy = oq1.getAttrList().get(0).getGroupByCondition().toStringWithoutTable();
//						System.out.println(groupBy);
						groupBy = QueryExecutor.FINAL_TABLE_GROUP_LIST;
						qe.showGroupQuery(cnt,groupBy);
//						qe.showGroupQueryResult(cnt,groupBy);
					}
					else if(groupBy.length()==0 && oq1.isCount())
						qe.showCountQueryResult(cnt);
					else if(groupBy.length()==0 && !oq1.isCount())
						qe.showNoGroupQuery(cnt);
					else 
						qe.showSimpleQueryResult(cnt);
					
					System.out.println(getRowCount("tmp")+ " rows in set");
					
/*					try
					{
						sp.emptyDB(Constants.USER_NAME, Constants.PASSWORD, Constants.site1, Constants.RESULT_SCHEMA_NAME);
						sp.emptyDB(Constants.USER_NAME, Constants.PASSWORD, Constants.site2, Constants.RESULT_SCHEMA_NAME);
						sp.emptyDB(Constants.USER_NAME, Constants.PASSWORD, Constants.site3, Constants.RESULT_SCHEMA_NAME);
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
					}
*/				}
				System.out.println("\nResponse Time :" + (System.currentTimeMillis() - t) + " ms.\n");
				/*
				 * ArrayList<Condition> arrconattr = new ArrayList<Condition>();int ret1=0; if (ret1 == 0) { System.out.println();
				 * //TSelectSqlStatement slcstmt = (TSelectSqlStatement)parser2.sqlstatements.get(0);
				 * //ModifyWhere w = new ModifyWhere(slcstmt.getWhereClause().getCondition(),0,arrcon,arrconattr); //w.printColumn();
				 * HashMap<String,ArrayList<String>> joinMap=new HashMap<String,ArrayList<String>>(); 
				 * Condition c1=new Condition(); Condition c2=new Condition(); Condition c3=new Condition(); 
				 * c1.setLHS("student1.rollNo");c1.setOperator("="); c1.setRHS("enrollment1.rollNo");
				 * c2.setLHS("enrollment1.oId"); c2.setOperator("=");c2.setRHS("courseOffering1.oId");
				 * c3.setLHS("courseOffering1.courseId"); c3.setOperator("=");c3.setRHS("course3.courseId"); 
				 * arrconattr.add(c1);arrconattr.add(c2); arrconattr.add(c3);
				 * for(Condition cond:arrconattr) { System.out.println(cond.getLHS()+" "+cond.getOperator()+" "+cond.getRHS()); 
				 * String lhs=cond.getLHS().split("\\.")[0]; String rhs=cond.getRHS().split("\\.")[0]; System.out.println(lhs+" "+rhs);
				 * if(!joinMap.containsKey(lhs)) { ArrayList<String> arr=new ArrayList<String>(); arr.add(rhs); joinMap.put(lhs,arr); }
				 * else { ArrayList<String> arr=joinMap.get(lhs); arr.add(rhs); joinMap.put(lhs,arr); }
				 * } //FromTableList ftl=Parser.getFromTableList(slcstmt);//ArrayList<Table> tab=ftl.getTableList(); int size=4;
				 * String[] tabArray={"student1","enrollment1","courseOffering1","course3"}; for(int i=0;i<size;i++) {
				 * //tabArray[i]=tab.get(i).getName(); } Perm p=new Perm(tabArray); String[][] res=p.callGenPerm();
				 * for(String sstr[]:res) { for(String str:sstr) System.out.print(str+" "); System.out.println(); } boolean[] bb=new boolean[res.length]; for(int i=0;i<res.length;i++) {
				 * String[] sstr=res[i]; bb[i]=false; for(int j=0;j<sstr.length-1;j++) {
				 * //System.out.print("#"+sstr[j]+" "+sstr[j+1]+" : "); if(joinMap.containsKey(sstr[j]) && joinMap.get(sstr[j]).contains(sstr[j+1])) { //
				 * System.out.println(true); bb[i]|=true; } else { //System.out.println(false); bb[i]|=false; } } if(bb[i]) {
				 * for(String str:sstr) System.out.print(str+" "); System.out.println(" : "+bb[i]); } } }
				 */
				clear();
			}
			catch (IOException ioe)
			{
				System.out.println("IO error trying to read your name!");
				System.exit(1);
			}
		}
		System.exit(0);
	}

	public static void clear()
	{
		DataLocalisation.localTableFragMap.clear();
		DataLocalisation.verticalFragStateMap.clear();
		SemanticUtility.tableDetailsMap.clear();
		SemanticUtility.tableFragmentListMap.clear();
		SemanticUtility.fragmentTableMap.clear();
		SemanticUtility.globalFragmentMap.clear();
		DataLocalisation.queryEnumerationList.clear();
		DataLocalisation.fromFragmentList.clear();
		DataLocalisation.selStatements.clear();
		OptimizeFragmentQuery.queryJoinCond.clear();
	}
	
	public static int getRowCount(String tableName)
	{
		
//		System.out.println("select cardinality from statistics where table_name='"+ tableName +"' and table_schema='"+ Constants.RESULT_SCHEMA_NAME +"'");
		Connection conn = DatabaseHandler.getConnection(Constants.URL_RESULT, SemanticUtility.USER_NAME, SemanticUtility.PASSWORD);
		int rowCnt = 0;

		try
		{
			PreparedStatement ps = conn.prepareStatement("select count(*) from tmp");
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
}
