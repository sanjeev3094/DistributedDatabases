package com.adb.general;

public class Constants
{
	public static final String HFRAG_TABLE = "hFrag";
	public static final String VFRAG_TABLE = "vFrag";
	public static final String DFRAG_TABLE = "dFrag";
	
	public static final String HOR_FRAG_TYPE = "HR";
	public static final String VER_FRAG_TYPE = "VR";
	public static final String DER_FRAG_TYPE = "DR";
	public static final String NO_FRAG_TYPE = "NF";
	
	public static final String HFRAG_COL_attr = "attr";
	public static final String HFRAG_COL_operator = "operator";
	public static final String HFRAG_COL_val = "val";
	
	public static final String VFRAG_COL_fragAttr = "fragAttr";
	
	public static final String DFRAG_COL_joinHFrag = "joinHFrag";
	public static final String DFRAG_COL_semiJoinAttr = "semiJoinAttr";

	public static final String SITE_COL_siteId = "siteId";
	public static final String SITE_COL_siteName = "siteName";
	public static final String SITE_COL_ipAddress = "ipAddress";
	public static final String SITE_COL_schemaName = "schemaName";
	public static final String SITE_COL_userName = "userName";
	public static final String SITE_COL_passwd = "passwd";
	
	public static final String FRAG_COL_fragName = "fragName";
	public static final String FRAG_COL_fragType = "fragType";
	public static final String FRAG_COL_relId = "relId";
	public static final String FRAG_COL_siteId = "siteId";
	
	public static final String REL_COL_relName = "relName";
	public static final String REL_COL_relId = "relId";
	
	public static final Integer ACCESSED_NOT_PRIMARY = 2;
	public static final Integer ACCESSED_PRIMARY = 1;
	
	public static final String URL_SYSCAT = "jdbc:mysql://localhost:3306/syscat" ;
	public static final String URL_COLLEGE = "jdbc:mysql://localhost:3306/college" ;
	public static final String URL_WORKSHOP = "jdbc:mysql://localhost:3306/workshop" ;
	public static final String URL_INFO_SCHEMA = "jdbc:mysql://localhost:3306/information_schema" ;
	public static final String URL_RESULT = "jdbc:mysql://localhost:3306/result" ;
	
	public static final String USER_NAME = "root" ;
	public static final String PASSWORD = "openstack" ;
	
	public static final String STUDENT_PREFIX = "student";
	public static final String ENROLLMENT_PREFIX = "enrollment";
	public static final String COURSE_PREFIX = "course";
	
	public static final String SEMIJOIN_SEP = ",";
	public static final String TRAILER_APPENDED = "_";
	public static final String RELATION_EQUALTIY = "=";
	
	public static final String TEMP_SCHEMA_NAME = "workshop";
	public static final String COLLEGE_SCHEMA = "college";
	public static final String RESULT_SCHEMA_NAME = "result";
	
	public static final String RESULT_TABLE_NAME = "final";
	
	public static final String site1 = "linux@10.2.24.197";
	public static final String site2 = "sanjeev@10.2.24.196";
	public static final String site3 = "localadmin@10.3.3.214";
}
