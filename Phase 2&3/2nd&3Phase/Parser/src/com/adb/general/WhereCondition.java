package com.adb.general;

public class WhereCondition
{
	Condition condition;
	String predicate;

	public String getPredicate()
	{
		return this.predicate;
	}

	public void setPredicate(String predicate)
	{
		this.predicate = predicate;
	}

	public String toString()
	{
		return this.predicate;
	}
}
