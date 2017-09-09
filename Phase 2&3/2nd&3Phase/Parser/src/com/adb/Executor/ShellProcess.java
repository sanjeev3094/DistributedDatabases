package com.adb.Executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.process.CommandExecutor;

public class ShellProcess
{
	String sshCmd="ssh -C ";
	String gzenc="gzip -9";
	String gzdec="gzip -d";
	String shell="/bin/bash";
	public void execute(String cmd) throws IOException, InterruptedException
	{
		List<String> commands = new ArrayList<String>();
		commands.add(shell);
		commands.add("-c");
		commands.add(cmd);
		CommandExecutor commandExecutor = new CommandExecutor(commands);
		commandExecutor.executeCommand();
		StringBuilder stdout = commandExecutor.getStandardOutputFromCommand();
		StringBuilder stderr = commandExecutor.getStandardErrorFromCommand();
		if(stdout.length()>0)
			System.out.println("Output:\n"+stdout);
		if(stderr.length()>0)
			System.out.println("Error:\n"+stderr);
	}
	public void execQuery(String user,String passwd,String onSite,String inDB,String query) throws IOException, InterruptedException
	{
		String userNPass="-u"+user+" -p"+passwd;
		String str="Executing query "+query+" in site "+onSite;
		String cmd="mysql "+userNPass+" "+"<<SQL \n"+
				"use "+inDB+"; \n"+
				query+";\n"+
				"quit \n"+
				"SQL";
		System.out.println(str);
		System.out.println(cmd);
		execute(cmd);
	}
	public void execQuery(String user,String passwd,String onSite,String inDB,String query,String toDB,String toTable) throws IOException, InterruptedException
	{
		String userNPass="-u"+user+" -p"+passwd;
		String str="Executing query "+query+" in site "+onSite+" to make relation "+toDB+"."+toTable;
		
		String cmd="mysql "+userNPass+" "+"<<SQL \n"+
				"use "+inDB+"; \n"+
				"drop table if exists "+toDB+"."+toTable+"; \n"+
				query+";\n"+
				"quit \n"+
				"SQL";
		if(onSite!=null)
			cmd=sshCmd+onSite+" \""+cmd+"\"";
		System.out.println(str);
//		System.out.println(cmd);
		execute(cmd);
	}
	public void transferRelation(String user,String passwd,String fromSite,String fromDB,String fromTable,String toSite,String toDB) throws IOException, InterruptedException
	{
		String userNPass="-u"+user+" -p"+passwd;
		String str="Transferring relation "+fromDB+"."+fromTable+" from site "+fromSite+" to relation "+toDB+"."+fromTable+" at site "+toSite;
		String fromcmd="mysqldump "+userNPass+" "+fromDB+" "+fromTable+"|"+gzenc;
		if(fromSite!=null)
			fromcmd=sshCmd+fromSite+" \""+fromcmd+"\"";
		fromcmd=fromcmd+"|"+gzdec+" |";
		String tocmd="mysql "+userNPass+" "+toDB;
		if(toSite!=null)
			tocmd=sshCmd+toSite+" \""+tocmd+"\"";
		String finalcmd=fromcmd+tocmd+";";
		System.out.println(str);
		execute(finalcmd);
	}
	public void getReducedWhereRelation(String user,String passwd,String inSite,String inDB,String inTable,String predicate,String toDB,String toTable) throws IOException, InterruptedException
	{
		String userNPass="-u"+user+" -p"+passwd;
		String cmd;
		
		String str="Copying relation "+inDB+"."+inTable+" in site "+inSite+" to relation "+toDB+"."+toTable;
		if (predicate!=null)
			str=str+" after applying predicate '"+predicate+"'";
		cmd="mysql "+userNPass+" "+"<<SQL \n"+
			"use "+toDB+"; \n"+
			"drop table if exists "+toTable+"; \n";
		if(predicate != null)
			cmd=cmd+"create table "+toTable+" as(SELECT * FROM "+inDB+"."+inTable+" WHERE "+predicate+");\n";
		else
			cmd=cmd+"create table "+toTable+" as(SELECT * FROM "+inDB+"."+inTable+");\n";
		cmd=cmd+"quit \n"+"SQL";
		if(inSite!=null)
			cmd=sshCmd+inSite+" \""+cmd+"\"";
		System.out.println(str);
		execute(cmd);
	}
	public void getJoinerRelation(String user,String passwd,String inSite,String inDB,String inTable,String attribute,String toDB,String toTable) throws IOException, InterruptedException
	{
		String userNPass="-u"+user+" -p"+passwd;
		String str="Copying relation "+inDB+"."+inTable+" in site "+inSite+" to relation "+toDB+"."+toTable+" after selecting attribute "+attribute;
		String cmd="mysql "+userNPass+" "+"<<SQL \n"+
				"use "+toDB+"; \n"+
				"drop table if exists "+toTable+"; \n"+
				"create table "+toTable+" as(SELECT distinct("+attribute+") FROM "+inDB+"."+inTable+");\n"+
				"quit \n"+
				"SQL";
		if(inSite!=null)
			cmd=sshCmd+inSite+" \""+cmd+"\"";
		System.out.println(str);
		execute(cmd);
	}
	public void renameRelation(String user,String passwd,String inSite,String inDB,String inTable,String toTable) throws IOException, InterruptedException
	{
		String userNPass="-u"+user+" -p"+passwd;
		String str="Renaming relation "+inDB+"."+inTable+" in site "+inSite+" to relation "+inDB+"."+toTable;
		String cmd="mysql "+userNPass+" "+"<<SQL \n"+
				"use "+inDB+"; \n"+
				"drop table if exists "+toTable+"; \n"+
				"rename table "+inTable+" to "+inDB+"."+toTable+";\n"+
				"quit \n"+
				"SQL";
		if(inSite!=null)
			cmd=sshCmd+inSite+" \""+cmd+"\"";
		System.out.println(str);
		execute(cmd);
	}
	public void deleteRelation(String user,String passwd,String inSite,String inDB,String inTable,String toTable) throws IOException, InterruptedException
	{
		String userNPass="-u"+user+" -p"+passwd;
		String str="Deleting relation "+inDB+"."+inTable+" in site "+inSite+" to relation "+inDB+"."+toTable;
		String cmd="mysql "+userNPass+" "+"<<SQL \n"+
				"use "+inDB+"; \n"+
				"drop table if exists "+toTable+"; \n"+
				"quit \n"+
				"SQL";
		if(inSite!=null)
			cmd=sshCmd+inSite+" \""+cmd+"\"";
		System.out.println(str);
		execute(cmd);
	}
	public void emptyDB(String user,String passwd,String inSite,String inDB) throws IOException, InterruptedException
	{
		String userNPass="-u"+user+" -p"+passwd;
		String str="Deleting database "+inDB+" in site "+inSite;
		String cmd="mysql "+userNPass+" "+"<<SQL \n"+
				"use "+inDB+"; \n"+
				"drop database if exists "+inDB+"; \n"+
				"create database "+inDB+"; \n"+
				"quit \n"+
				"SQL";
		if(inSite!=null)
			cmd=sshCmd+inSite+" \""+cmd+"\"";
		System.out.println(str);
		execute(cmd);
	}

	public void getReducedFirstTable(String user,String passwd,String onSite,String inDB,String firstTable,String secondTable,String predicate,String resTable) throws IOException, InterruptedException
	{
		String userNPass="-u"+user+" -p"+passwd;
		String str="Joining relations "+inDB+"."+firstTable+" and "+inDB+"."+secondTable+" on site "+onSite+" to relation "+inDB+"."+resTable;
		if (predicate!=null)
			str=str+" after applying predicate '"+predicate+"'";

		String cmd="mysql "+userNPass+" "+"<<SQL \n"+
				"use "+inDB+"; \n"+
				"drop table if exists "+resTable+"; \n"+
				"create table "+resTable+" as(SELECT "+firstTable+".* FROM "+inDB+"."+firstTable+","+inDB+"."+secondTable+" WHERE "+predicate+");\n"+
				"quit \n"+
				"SQL";
		if(onSite!=null)
			cmd=sshCmd+onSite+" \""+cmd+"\"";
		System.out.println(str);
		execute(cmd);
	}
	
	public static void main(String args[])
	{
/*		ShellProcess sp=new ShellProcess();
		try
		{
			String spp="select * from student1;";
			sp.execQuery("root","openstack","linux@10.1.66.37","workshop",spp);
//			sp.emptyDB("root","openstack","linux@10.1.66.37","workshop");
//			sp.emptyDB("root","openstack","localadmin@10.3.3.214","workshop");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
*/	}
}
