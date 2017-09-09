package com.adb.Executor.SDD1;

import java.util.ArrayList;

public class SDDImplTest
{
	public static void main(String[] args)
	{
		DatabaseProfile dbProfile = new DatabaseProfile();
		
		dbProfile.getRelationSizeMap().put("R1", 1500.0);
		dbProfile.getRelationSizeMap().put("R2", 3000.0);
		dbProfile.getRelationSizeMap().put("R3", 2000.0);
		
		dbProfile.getAttrDetailMap().put("R2.A,R1.A", new SelectivityFactorAndSizeAttr(.3, 36.0));
		dbProfile.getAttrDetailMap().put("R1.A,R2.A", new SelectivityFactorAndSizeAttr(.8, 320.0));
		dbProfile.getAttrDetailMap().put("R3.B,R2.B", new SelectivityFactorAndSizeAttr(1.0, 400.0));
		dbProfile.getAttrDetailMap().put("R2.B,R3.B", new SelectivityFactorAndSizeAttr(.4, 80.0));
		
		ArrayList<String> totalList = new ArrayList<String>();
		totalList.add("R1.A,R2.A");
		totalList.add("R2.A,R1.A");
		totalList.add("R3.B,R2.B");
		totalList.add("R2.B,R3.B");
		
		SDDImplementor sdd = new SDDImplementor();
		sdd.initiateSDD(dbProfile, totalList);
	}
}
