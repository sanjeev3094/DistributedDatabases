package participants.threads;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.JMSException;

import participants.Coordinator;
import pojo.TransactionParticipant;

import com.ias.client.jms.Consumer;

public class RecieveAckFromValidParty implements Runnable
{
	final String ACK_RECIEVED = "ACK";
	private static Logger LOGGER;

	TransactionParticipant coordinator;
	
	public RecieveAckFromValidParty(TransactionParticipant coordinator, Logger logger)
	{
		this.coordinator = coordinator;
		LOGGER=logger;
	}

	@Override
	public void run()
	{
		StringBuffer msgFrom = new StringBuffer("");
		try
		{
			Consumer consumer = new Consumer();
			System.out.println(">>>"+coordinator.getIpAddress() + ":" + coordinator.getPortNo());
			consumer.CreateConnection(coordinator.getIpAddress() + ":" + coordinator.getPortNo(),"");
			
			String resMsg = (String)consumer.ReceiveMessage(msgFrom);
			System.out.println("RECVD : "+ resMsg +" FROM "+msgFrom);
			consumer.CloseConnection();
			
			if (resMsg.equals(ACK_RECIEVED))
			{
				LOGGER.setLevel(Level.ALL);
				LOGGER.info("Recieved ACK from [" + msgFrom +"]");
			}
			else
			{
				Coordinator.allAck = false;
			}
		}
		catch(NullPointerException npe)
		{
			System.out.println("No ack...");
			Coordinator.allAck = false;
		}
		catch (JMSException e)
		{
			e.printStackTrace();
		}
	}
}
