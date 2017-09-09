package com.adb.general;

public class Site
{
	Integer siteId;
	String siteName;
	String ipAddress;
	String schemaName;
	String userName;
	String passwd;
	
	public Site()
	{
		super();
	}

	public Site(Integer siteId, String siteName, String ipAddress,String schemaName, String userName, String passwd)
	{
		super();
		this.siteId = siteId;
		this.siteName = siteName;
		this.ipAddress = ipAddress;
		this.schemaName = schemaName;
		this.userName = userName;
		this.passwd = passwd;
	}

	public Integer getSiteId()
	{
		return siteId;
	}

	public void setSiteId(Integer siteId)
	{
		this.siteId = siteId;
	}

	public String getSiteName()
	{
		return siteName;
	}

	public void setSiteName(String siteName)
	{
		this.siteName = siteName;
	}

	public String getIpAddress()
	{
		return ipAddress;
	}

	public void setIpAddress(String ipAddress)
	{
		this.ipAddress = ipAddress;
	}

	public String getSchemaName()
	{
		return schemaName;
	}

	public void setSchemaName(String schemaName)
	{
		this.schemaName = schemaName;
	}

	public String getUserName()
	{
		return userName;
	}

	public void setUserName(String userName)
	{
		this.userName = userName;
	}

	public String getPasswd()
	{
		return passwd;
	}

	public void setPasswd(String passwd)
	{
		this.passwd = passwd;
	}
}
