package com.adb.general;

public class AggregateFunction
{
	public static boolean isIntegerType(String func)
	{
		if (isCount(func.toLowerCase()) || isSum(func.toLowerCase()))
			return true;
		return false;
	}

	public static boolean isAvg(String func)
	{
		if (func.toLowerCase().startsWith("avg"))
			return true;
		return false;
	}

	public static boolean isCount(String func)
	{
		if (func.toLowerCase().startsWith("count"))
			return true;
		return false;
	}

	public static boolean isSum(String func)
	{
		if (func.toLowerCase().startsWith("sum"))
			return true;
		return false;
	}

	public static boolean isMax(String func)
	{
		if (func.toLowerCase().startsWith("max"))
			return true;
		return false;
	}

	public static boolean isMin(String func)
	{
		if (func.toLowerCase().startsWith("min"))
			return true;
		return false;
	}
}
