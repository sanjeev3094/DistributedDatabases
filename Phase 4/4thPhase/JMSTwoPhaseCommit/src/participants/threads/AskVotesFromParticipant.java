package participants.threads;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.JMSException;

import participants.Coordinator;
import pojo.TransactionParticipant;

import com.ias.client.jms.Consumer;

public class AskVotesFromParticipant implements Runnable
{
	final String SEND_PREPARE = "PREPARE";
	final String VOTE_COMMIT_RECIEVED = "VOTE-COMMIT";
	final String VOTE_ABORT_RECIEVED = "VOTE-ABORT";
	private Logger LOGGER;
	
	private Consumer consumer;
	
	public AskVotesFromParticipant(){}
	
	public AskVotesFromParticipant(Consumer consumer,Logger logger)
	{
		this.consumer = consumer;
		this.LOGGER = logger;
	}
	
	public void stop()
	{
		
	}
	
	@Override
	public void run()
	{
		StringBuffer msgFrom =new StringBuffer("");
		try
		{
			/*Consumer consumer = new Consumer();
			System.out.println(":::"+coordinator.getIpAddress() + ":" + coordinator.getPortNo());
			consumer.CreateConnection(coordinator.getIpAddress() + ":" + coordinator.getPortNo(),"");*/
			
			String resultMsg = (String)consumer.ReceiveMessage(msgFrom);
			
			System.out.println("RECVD : "+ resultMsg +" FROM " + msgFrom);
//			consumer.CloseConnection();
			
			if (resultMsg.equals(VOTE_COMMIT_RECIEVED))
			{
				
//				System.out.println("Vote commit recieved from [" + socket.getInetAddress().getHostAddress() + ":" + socket.getPort()+"]");
				LOGGER.setLevel(Level.ALL);
				LOGGER.info("Vote commit recieved from [" + msgFrom.toString() +"]");
//				String[] pt = msgFrom.toString().split(":");
//				TransactionParticipant party = new TransactionParticipant(-1,false,pt[0],Integer.parseInt(pt[1]));
//				Coordinator.partiesVotedCommit.add(party);
			}
			else
			{
//				System.out.println("Vote abort recieved from [" + socket.getInetAddress().getHostAddress() + ":" + socket.getPort()+"]");
				LOGGER.setLevel(Level.WARNING);
				LOGGER.warning("Vote abort recieved from [" + msgFrom +"]");
				String[] pt = msgFrom.toString().split(":");
				TransactionParticipant party = new TransactionParticipant(-1,false,pt[0],Integer.parseInt(pt[1]));
				Coordinator.partiesVotedAbort.add(party);
				Coordinator.toCommit = false;
			}
		}
		catch(NullPointerException npe)
		{
//			System.out.println("Disconnected from the participant [" + socket.getInetAddress().getHostAddress() + ":" + socket.getPort()+"]");
			LOGGER.setLevel(Level.WARNING);
			LOGGER.warning("Disconnected from the participant [" + msgFrom +"]");
			Coordinator.toCommit = false;
		}
		catch (JMSException e)
		{
			e.printStackTrace();
		}
	}
}
