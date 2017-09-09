package com.adb.Executor.SDD1;

import java.io.Serializable;

public class DbProfileRelationDetails implements Serializable
{
	private static final long serialVersionUID = 1L;
	int card;
	int tupleSize;
	Double relationSize;
	
	public DbProfileRelationDetails()
	{
		super();
	}

	public DbProfileRelationDetails(int card, int tupleSize, Double relationSize)
	{
		super();
		this.card = card;
		this.tupleSize = tupleSize;
		this.relationSize = relationSize;
	}

	public int getCard()
	{
		return card;
	}

	public void setCard(int card)
	{
		this.card = card;
	}

	public int getTupleSize()
	{
		return tupleSize;
	}

	public void setTupleSize(int tupleSize)
	{
		this.tupleSize = tupleSize;
	}

	public Double getRelationSize()
	{
		return relationSize;
	}

	public void setRelationSize(Double relationSize)
	{
		this.relationSize = relationSize;
	}
}
