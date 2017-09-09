package com.adb.Executor;

import java.util.ArrayList;

public class Test
{

	public static void main(String s[])
	{
		ServicesUtitlity fs = new ServicesUtitlity();
		ArrayList<String> fragments = new ArrayList<String>();
		fragments.add("course2");
		fragments.add("enrollment2");
		fragments.add("faculty2");
		fragments.add("student2");
		fs.findStatisticsOfFragments(fragments);

		
		
	}
}
