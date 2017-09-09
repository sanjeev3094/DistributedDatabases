package com.adb.general;

public class Condition 
{
	private String lhs;
	private String operator;
	private String rhs;
	public Condition()
	{
		
	}
	public Condition(String lhs,String op,String rhs)
	{
		setLHS(lhs);
		setRHS(rhs);
		setOperator(op);
	}
	public String getLHS()
	{
		return this.lhs;
	}
	public void setLHS(String lhs)
	{
		this.lhs=lhs;
	}
	public String getRHS()
	{
		return this.rhs;
	}
	public void setRHS(String rhs)
	{
		this.rhs=rhs;
	}
	public String getOperator()
	{
		return this.operator;
	}
	public void setOperator(String op)
	{
		this.operator=op;
	}
	public String toString()
	{
		return lhs+operator+rhs;
	}
}
