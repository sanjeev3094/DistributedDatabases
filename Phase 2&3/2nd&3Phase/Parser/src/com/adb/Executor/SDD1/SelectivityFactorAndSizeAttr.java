package com.adb.Executor.SDD1;

import java.io.Serializable;

public class SelectivityFactorAndSizeAttr implements Serializable
{
	private static final long serialVersionUID = -8429063083046674404L;
	Double selectivityFactor;
	Double sizeAttr;
	
	public SelectivityFactorAndSizeAttr()
	{
		super();
	}
	public SelectivityFactorAndSizeAttr(Double selectivityFactor, Double sizeAttr)
	{
		super();
		this.selectivityFactor = selectivityFactor;
		this.sizeAttr = sizeAttr;
	}
	public Double getSelectivityFactor()
	{
		return selectivityFactor;
	}
	public void setSelectivityFactor(Double selectivityFactor)
	{
		this.selectivityFactor = selectivityFactor;
	}
	public Double getSizeAttr()
	{
		return sizeAttr;
	}
	public void setSizeAttr(Double sizeAttr)
	{
		this.sizeAttr = sizeAttr;
	}
}
