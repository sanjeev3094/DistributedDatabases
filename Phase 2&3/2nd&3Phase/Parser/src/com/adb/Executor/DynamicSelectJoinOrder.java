package com.adb.Executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.adb.Executor.SDD1.FragmentStatistics;

public class DynamicSelectJoinOrder
{
	ArrayList<FragmentStatistics> relList = new ArrayList<FragmentStatistics>();
	HashMap<String,Integer> sizeMap = new HashMap<String, Integer>();
	HashMap<String,Integer> interCostMap = new HashMap<String, Integer>();
	HashMap<String,String> bestPlanMap = new HashMap<String, String>();
	HashMap<String,Integer> VRAttrMap = new HashMap<String, Integer>();

	public ArrayList<FragmentStatistics> getRelList()
	{
		return relList;
	}

	public void setRelList(ArrayList<FragmentStatistics> relList)
	{
		this.relList = relList;
	}

	public HashMap<String, Integer> getSizeMap()
	{
		return sizeMap;
	}

	public void setSizeMap(HashMap<String, Integer> sizeMap)
	{
		this.sizeMap = sizeMap;
	}

	public HashMap<String, Integer> getInterCostMap()
	{
		return interCostMap;
	}

	public void setInterCostMap(HashMap<String, Integer> interCostMap)
	{
		this.interCostMap = interCostMap;
	}

	public HashMap<String, String> getBestPlanMap()
	{
		return bestPlanMap;
	}

	public void setBestPlanMap(HashMap<String, String> bestPlanMap)
	{
		this.bestPlanMap = bestPlanMap;
	}

	public HashMap<String, Integer> getVRAttrMap()
	{
		return VRAttrMap;
	}

	public void setVRAttrMap(HashMap<String, Integer> vRAttrMap)
	{
		VRAttrMap = vRAttrMap;
	}

	int calculateCost(ArrayList<FragmentStatistics> otherRelList,FragmentStatistics remOneRelation)
	{
		String rel1Name = remOneRelation.getName();
		ArrayList<String> atts1 = remOneRelation.getAttrList();
		
		System.out.println("Rem Relation : " + rel1Name);
		// find common attribute
		
		String commonAttr = null;
		String rel2Name = null;
		
		for(String att1 : atts1)
		{
			for(FragmentStatistics rel:otherRelList)
			{
				ArrayList<String> atts2 = rel.getAttrList();
				
				for(String att2 : atts2)
				{
					if(att1.equalsIgnoreCase(att2))
					{
						rel2Name = rel.getName();
						commonAttr = att1;
						break;
					}
				}
				if(commonAttr != null)
					break;
			}
			if(commonAttr != null)
				break;
		}
		
		String remOneName = "(" + remOneRelation.getName() + ")";
		
		String others = "(";
		
		for(int j=0;j<otherRelList.size();++j)
		{
			others += otherRelList.get(j).getName();
			if(j<otherRelList.size()-1)
				others += ",";
		}
		
		others += ")";
		
		int left = sizeMap.get(others);
		int right = sizeMap.get(remOneName);
		
		int div = 1;

		// find Max(V(R,a),V(S,a))
		if(commonAttr != null)
		{
			System.out.println("("+ rel2Name +","+commonAttr+")");
			System.out.println("("+ rel1Name +","+commonAttr+")");
			int x1 = VRAttrMap.get("("+ rel2Name +","+commonAttr+")");
			int x2 = VRAttrMap.get("("+ rel1Name +","+commonAttr+")");
			div = Math.max(x1,x2);
		}
			
		return left*right/div;
	}
	
	void fillRelList()
	{
		ArrayList<String> attrL1 = new ArrayList<String>();
		attrL1.add("a");
		attrL1.add("b");
		FragmentStatistics r1 = new FragmentStatistics();
		r1.setName("S");
		r1.setAttrList(attrL1);
		r1.setNoOfTuples(1000);
		ArrayList<String> attrL2 = new ArrayList<String>();
		attrL2.add("b");attrL2.add("c");
		FragmentStatistics r2 = new FragmentStatistics();
		r2.setName("S");
		r2.setAttrList(attrL2);
		r2.setNoOfTuples(1000);
		ArrayList<String> attrL3 = new ArrayList<String>();
		attrL3.add("c");attrL3.add("d");
		FragmentStatistics r3 = new FragmentStatistics();
		r3.setName("S");
		r3.setAttrList(attrL2);
		r3.setNoOfTuples(1000);
		ArrayList<String> attrL4 = new ArrayList<String>();
		attrL4.add("d");attrL4.add("a");
		FragmentStatistics r4 = new FragmentStatistics();
		r4.setName("S");
		r4.setAttrList(attrL2);
		r4.setNoOfTuples(1000);
		relList.add(r1);relList.add(r2);relList.add(r3);relList.add(r4);
		
		// fill the details
		VRAttrMap.put("(R,a)", 100);
		VRAttrMap.put("(R,b)", 200);
		VRAttrMap.put("(S,b)", 100);
		VRAttrMap.put("(S,c)", 500);
		VRAttrMap.put("(T,c)", 20);
		VRAttrMap.put("(T,d)", 50);
		VRAttrMap.put("(U,a)", 50);
		VRAttrMap.put("(U,d)", 1000);
	}
	
	void generate(FragmentStatistics[] relationObjList,int c,int start,int l,int n)
	{
		if(c==l)
		{
			String joinString = "(";
			for(int i=0;i<c;++i)
			{
				joinString += relationObjList[i].getName();
				if(i<c-1)
					joinString = joinString + ",";
			}
			joinString = joinString + ")";

			//
			if(c==1)
			{
				System.out.println("calculate best cost for : " + joinString);
				sizeMap.put(joinString, 1000);
				interCostMap.put(joinString, 0);
				bestPlanMap.put(joinString,joinString);
			}
			if(c==2)
			{
				System.out.println("calculate best cost for : " + joinString);
				// calculate cost
				ArrayList<FragmentStatistics> otherList = new ArrayList<FragmentStatistics>();
				otherList.add(relationObjList[0]);
				int cost = calculateCost(otherList,relationObjList[1]);
				sizeMap.put(joinString, cost);
				interCostMap.put(joinString, 0);
				bestPlanMap.put(joinString,joinString);
			}
			if(c>2)
			{
				System.out.println("calculate best cost for : " + joinString);
				
				ArrayList<FragmentStatistics> otherRelList = new ArrayList<FragmentStatistics>();
				FragmentStatistics remOneObj;
				
				for(int i=0;i<c;++i)
				{
					String leftOne = relationObjList[i].getName();
					remOneObj = relationObjList[i];
					String others = "(";
					int cnt = 0;
					for(int j=0;j<c;++j)
					{
						if(j==i)
							continue;
						others += relationObjList[j].getName();
						otherRelList.add(relationObjList[j]);
						cnt++;
						if(cnt<c-1)
							others += ",";
					}
					others += ")";
					
					String finalS =  others + "," + leftOne ; 
					System.out.println("\n" + finalS);
					
					int cost = calculateCost(otherRelList,remOneObj);
					int interCost = sizeMap.get(others) + interCostMap.get(others);
					System.out.println( "\n" + joinString + " : " +  finalS + " : " + cost + " interCost" + " : " + interCost);
					
					if(interCostMap.containsKey(joinString))	// update the cost if less
					{
						if(interCostMap.get(joinString) > interCost)
						{
							sizeMap.put(joinString, cost);
							interCostMap.put(joinString, interCost);
							bestPlanMap.put(joinString, finalS);
						}
					}
					else
					{
						sizeMap.put(joinString, cost);
						interCostMap.put(joinString, interCost);
						bestPlanMap.put(joinString,finalS);
					}
					otherRelList.clear();
				}
				System.out.println("best cost : "+ interCostMap.get(joinString));
			}
			return;
		}
		
		for(int i=start; i<n;++i)
		{
			relationObjList[c] = relList.get(i);
			generate(relationObjList,c+1,i+1,l,n);
		}
	}

	void printMap()
	{
		for(Map.Entry<String, Integer> entry:sizeMap.entrySet())
		{
			System.out.println("Size  " + entry.getKey() + " = " + entry.getValue());
		}
		
		for(Map.Entry<String, Integer> entry:interCostMap.entrySet())
		{
			System.out.println("Intermediate Cost " + entry.getKey() + " = " + entry.getValue());
		}
		for(Map.Entry<String, String> entry:bestPlanMap.entrySet())
		{
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
	}
	
	public static void main(String args[])
	{
		int n=4;
		DynamicSelectJoinOrder d = new DynamicSelectJoinOrder();
		d.fillRelList();
		
		FragmentStatistics r[] = new FragmentStatistics[n];
		
		for(int l=1;l<=n;++l)
			d.generate(r, 0, 0, l, n);
		
		d.printMap();
	}
}
