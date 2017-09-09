package com.adb.Executor;

import com.adb.Executor.SDD1.FragmentStatistics;

public class SystemR
{
	public void systemRjoinOrder(Statistics stSet)
	{
		DynamicSelectJoinOrder joinOrder = new DynamicSelectJoinOrder();
		joinOrder.setVRAttrMap(stSet.getVRAttrMap());
		
		int n=4;
		FragmentStatistics r[] = new FragmentStatistics[n];

		for(int l=1;l<=n;++l)
			joinOrder.generate(r, 0, 0, l, n);
		
		
	}
}
