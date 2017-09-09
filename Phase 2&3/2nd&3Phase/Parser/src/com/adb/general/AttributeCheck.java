package com.adb.general;

import com.adb.utils.SemanticUtility;

public class AttributeCheck
{
	public static Attribute check(String col)
	{
		String attrS[] = SemanticUtility.aggregateFnAndAttr(col);
		String function = attrS[0];
		String strattr = attrS[1];
		Attribute attr = new Attribute();
		//System.out.println(strattr+function);
		if (strattr.contains("."))
		{
			String splitAttr[] = strattr.split("\\.");
			String tableName = splitAttr[0];
			String attribute = splitAttr[1];
			//System.out.println(attr+attribute+tableName+function);
			int retval = SemanticUtility.checkAttribute(attr, attribute,tableName, function);
			if (retval != 0)
			{
				if (retval == 1)
					System.out.println("Attribute " + attribute + " not found in the table " + tableName);
				else if (retval == -1)
					System.out.println("Table " + tableName + " not found.");
				System.exit(1);
			}
		}
		else
		{
			int retval = SemanticUtility.checkAttribute(attr, strattr, function);
			if (retval != 0)
			{
				if (retval == 1)
					System.out.println("Ambiguous attribute " + strattr);
				else if (retval == -1)
					System.out.println("Attribute " + strattr + " not found in tables.");
				System.exit(1);
			}
		}
		return attr;
	}
}
