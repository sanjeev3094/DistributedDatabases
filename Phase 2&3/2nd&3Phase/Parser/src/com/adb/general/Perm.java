package com.adb.general;
public class Perm
{
	String[] in;
	String[][] result;
	int nfact;
	
	public int cnt;
	public Perm(String[] strs)
	{
		this.in=strs;
		cnt=0;
	}
	public void genPerm(String[] out,boolean [] used,int n,int d)
	{
		if(d==in.length)
		{
			result[cnt]=out.clone();
			cnt++;
			return;
		}
		for(int i=0;i<n;i++)
		{
			if(used[i]==true)
				continue;
			out[d]=in[i];
			used[i]=true;
			genPerm(out,used,n,d+1);
			used[i]=false;
		}
	}
	public String[][] callGenPerm()
	{
		int nfact=fact(in.length);
		result=new String[nfact][in.length];
		for(int i=0;i<nfact;i++)
			result[i]=new String[in.length];
		String[] strs1=new String[in.length];
		boolean[] b=new boolean[in.length];
		for(int i=0;i<in.length;i++)
			b[i]=false;
		genPerm(strs1,b,in.length,0);
		return result;
	}
	public static void main(String args[])
	{
		String[] strs={"Student1","Student2","Student3"};
		
		
		Perm p=new Perm(strs);
		String[][] res=p.callGenPerm();
		for(String sstr[]:res)
		{
			for(String str:sstr)
				System.out.print(str);
			System.out.println();
		}


	}
	public static int fact(int n)
	{
		if(n==0 || n==1)
			return 1;
		else 
			return n*fact(n-1);
	}
}
