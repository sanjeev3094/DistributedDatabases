package pojo;

import java.io.Serializable;

public class TransactionParticipant implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	protected int priority;
	protected boolean isCoordinator;
	protected String ipAddress;
	protected int portNo;
	
	public TransactionParticipant()
	{
		super();
	}

	public TransactionParticipant(int priority, boolean isCoordinator, String ipAddress, int portNo)
	{
		super();
		this.priority = priority;
		this.isCoordinator = isCoordinator;
		this.ipAddress = ipAddress;
		this.portNo = portNo;
	}

	public int getPriority()
	{
		return priority;
	}

	public void setPriority(int priority)
	{
		this.priority = priority;
	}

	public boolean isCoordinator()
	{
		return isCoordinator;
	}

	public void setCoordinator(boolean isCoordinator)
	{
		this.isCoordinator = isCoordinator;
	}

	public String getIpAddress()
	{
		return ipAddress;
	}

	public void setIpAddress(String ipAddress)
	{
		this.ipAddress = ipAddress;
	}

	public int getPortNo()
	{
		return portNo;
	}

	public void setPortNo(int portNo)
	{
		this.portNo = portNo;
	}
	
	public int hashCode()
	{
		return ipAddress.length()*20;
	}
	
	public boolean equals(Object obj)
	{
		if(this == obj)
			return true;
		if(obj == null || obj.getClass() != this.getClass())
			return false;
		
		TransactionParticipant party = (TransactionParticipant) obj;
		if(this.ipAddress.equalsIgnoreCase(party.ipAddress) && this.portNo == party.portNo)
			return true;
		else
			return false;
	}
}
