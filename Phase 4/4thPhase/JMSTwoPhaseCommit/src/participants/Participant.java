package participants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.JMSException;

import pojo.TransactionParticipant;

import com.ias.client.jms.Consumer;
import com.ias.client.jms.Producer;

public class Participant
{
	final String PREPARE_MSG_RECIEVED = "PREPARE";
	final String SEND_VOTE_COMMIT = "VOTE-COMMIT";
	final String SEND_VOTE_ABORT = "VOTE-ABORT";
	final String GLOBAL_COMMIT_RECIEVED = "GLOBAL-COMMIT";
	final String GLOBAL_ABORT_RECIEVED = "GLOBAL-ABORT";
	final String SEND_ACK = "ACK";
	private static Logger LOGGER;
	
	enum states
	{
		INITIAL, READY, COMMIT, ABORT
	};

	states state;
	String tagParticipant;

	public void init(TransactionParticipant coordinator, ArrayList<TransactionParticipant> participantList, ServerSocket server, TransactionParticipant party, Logger logger)
	{
		state = states.INITIAL;
		LOGGER = logger;
		tagParticipant = party.getIpAddress()+":"+party.getPortNo();
		try
		{
			Consumer consumer = new Consumer();
			System.out.println("///" + party.getIpAddress() + ":" + party.getPortNo());
			consumer.CreateConnection(party.getIpAddress() + ":" + party.getPortNo(),"");
			String resMsg = (String)consumer.ReceiveMessage();
			System.out.println("RECVD : "+ resMsg);
			
			consumer.CloseConnection();
			
			if (resMsg.equals(PREPARE_MSG_RECIEVED))
			{
//				System.out.println("<<PREPARE>> recieved from [" + socket.getInetAddress().getHostAddress() + ":" + socket.getPort()+"]");
				LOGGER.setLevel(Level.ALL);
				LOGGER.info("<<PREPARE>> recieved from [" + coordinator.getIpAddress() + ":" + coordinator.getPortNo() +"]");

				if (promptReady(coordinator))
				{
					// write ready in Log
					
					state = states.READY;
					LOGGER.setLevel(Level.ALL);
					LOGGER.info("In ready state and waiting for global commit or abort");

					waitForMessageFromCoordinator(coordinator, party);					
				}
				else
				{
					// write abort in Log (unilateral abort)
					
//					System.out.println("In abort state");
					LOGGER.setLevel(Level.WARNING);
					LOGGER.warning("In abort state");
					state = states.ABORT;
				}
			}
			
//			System.out.println("Interaction over with co-ordinator...");
			LOGGER.setLevel(Level.ALL);
			LOGGER.info("Interaction over with co-ordinator...");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch(InterruptedException ie)
		{
			ie.printStackTrace();
		}
		catch (JMSException e)
		{
			e.printStackTrace();
		}
	}

	private boolean promptReady(TransactionParticipant coordinator) throws JMSException
	{
		System.out.print("Ready to commit ??? ");
		
		try
		{
			/*FileInputStream finpStream = new FileInputStream(new File("input.txt"));
			DataInputStream in = new DataInputStream(finpStream);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));*/
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			
			if (reader.readLine().toUpperCase().equals("Y"))
			{
				System.out.println("commiting...");
				
				// write ready in log
				Producer pro = new Producer();
				pro.CreateConnection(coordinator.getIpAddress() + ":" + coordinator.getPortNo());
				pro.SendObjectMessage(SEND_VOTE_COMMIT, tagParticipant);
				pro.CloseConnection();
				
//				System.out.println("Sending <<"+SEND_VOTE_COMMIT+">>");
				LOGGER.setLevel(Level.ALL);
				LOGGER.info("Sending <<"+SEND_VOTE_COMMIT+">>");
				LOGGER.setLevel(Level.ALL);
				LOGGER.info("In <<COMMIT>> state..");

				return true;
			}
			else
			{
				System.out.println("aborting...");
				
				// write abort in log
				Producer pro = new Producer();
				pro.CreateConnection(coordinator.getIpAddress() + ":" + coordinator.getPortNo());
				pro.SendObjectMessage(SEND_VOTE_ABORT,tagParticipant);
				pro.CloseConnection();
				
//				System.out.println("Sending <<"+SEND_VOTE_ABORT+">>");
				LOGGER.setLevel(Level.WARNING);
				LOGGER.warning("Sending''' <<"+SEND_VOTE_ABORT+">>");
				LOGGER.setLevel(Level.WARNING);
				LOGGER.warning("In <<ABORT>> state..");

				return false;
			}
		}
		catch (IOException e)
		{
			System.out.println("IOError in Participant...");
			e.printStackTrace();
		}
		return false;
	}

	private void waitForMessageFromCoordinator(TransactionParticipant coordinator,TransactionParticipant party) throws IOException, InterruptedException, JMSException
	{
		Consumer consumer = new Consumer();
		System.out.println(party.getIpAddress() + ":" + party.getPortNo());
		consumer.CreateConnection(party.getIpAddress() + ":" + party.getPortNo(),"");
		
		String resMsg = (String)consumer.ReceiveMessage();
		System.out.println("RECVD : "+ resMsg);
		
		consumer.CloseConnection();

		
		if (resMsg.equals(GLOBAL_COMMIT_RECIEVED))
		{
//			System.out.println("Global commit recieved...");
			LOGGER.setLevel(Level.ALL);
			LOGGER.info("Global commit recieved...");
			// write commit in log
			Producer pro = new Producer();
			pro.CreateConnection(coordinator.getIpAddress() + ":" + coordinator.getPortNo());
			pro.SendObjectMessage(SEND_ACK, tagParticipant);
			pro.CloseConnection();
			
//			System.out.println("Sending ACK to coordinator...");
			LOGGER.setLevel(Level.ALL);
			LOGGER.info("Sending ACK to coordinator...");
			state = states.COMMIT;
		}
		else
		{
//			System.out.println("Global Abort recieved...");
			LOGGER.setLevel(Level.WARNING);
			LOGGER.warning("Global Abort recieved...");
			// write abort in the log
			Producer pro = new Producer();
			pro.CreateConnection(coordinator.getIpAddress() + ":" + coordinator.getPortNo());
			pro.SendObjectMessage(SEND_ACK, tagParticipant);
			pro.CloseConnection();

//			System.out.println("Sending ACK to coordinator...");
			LOGGER.setLevel(Level.ALL);
			LOGGER.info("Sending ACK to coordinator...");
			state = states.ABORT;
		}
	}
}
