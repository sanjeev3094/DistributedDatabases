package participants;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.JMSException;

import participants.threads.AskVotesFromParticipant;
import participants.threads.RecieveAckFromValidParty;
import pojo.TransactionParticipant;

import com.ias.client.jms.Consumer;
import com.ias.client.jms.Producer;

public class Coordinator
{
	final Integer TIMEOUT = 20000;
	final String SEND_PREPARE = "PREPARE";
	final String SEND_GLOBAL_COMMIT = "GLOBAL-COMMIT";
	final String SEND_GLOBAL_ABORT = "GLOBAL-ABORT";
	final String ACK_RECIEVED = "ACK";
	final String VOTE_COMMIT_RECIEVED = "VOTE-COMMIT";
	final String VOTE_ABORT_RECIEVED = "VOTE-ABORT";

	enum states
	{
		INITIAL,WAIT, COMMIT, ABORT
	};

	public static states state;
	public static boolean toCommit;
	public static boolean allAck;
	public static boolean resendGlobalMsg;
	private static Logger LOGGER;
	public static ArrayList<TransactionParticipant> parties = null;
	public static ArrayList<TransactionParticipant> partiesVotedCommit = new ArrayList<TransactionParticipant>();
	public static ArrayList<TransactionParticipant> partiesVotedAbort = new ArrayList<TransactionParticipant>();
	public static ArrayList<Socket> partiesDontAck = new ArrayList<Socket>();
	ArrayList<Thread> threadList = new ArrayList<Thread>();
	
	public void init(TransactionParticipant coordinator, ArrayList<TransactionParticipant> participantList, Logger logger)
	{
		parties = participantList;
		state = states.INITIAL;
		LOGGER = logger;

		try
		{
			broadcastPrepareMsg();
			state = states.WAIT;
//			System.out.println("In wait state and waiting for votes from participant...");
			LOGGER.setLevel(Level.ALL);
			LOGGER.info("In wait state and waiting for votes from participant...");

			toCommit = true;
			
			spawnsVoteRecThreads(coordinator);// spawn Threads and recieve votes from participants
			
			String message;
			if (toCommit)
			{
				message = SEND_GLOBAL_COMMIT;
				// write commit in Log file 
			}
			else
			{
				message = SEND_GLOBAL_ABORT;
				// write abort in Log file
			}
			
			if(parties.size()>0)
				sendGlobalMsg(message);
			
			if(toCommit)
			{
				state = states.COMMIT;
//				System.out.println("In commit state...");
				LOGGER.setLevel(Level.ALL);
				LOGGER.info("In commit state...");
			}
			else
			{
				state = states.ABORT;
//				System.out.println("In abort state...");
				LOGGER.setLevel(Level.ALL);
				LOGGER.info("In abort state...");
			}


			allAck = true;
			resendGlobalMsg = false;
	
			recieveAck(coordinator);
//			spawnAckRecThreads(coordinator);	//spawns the thread for acknowledgement
			
			/*while(partiesDontAck.size() != 0)
			{
				resendGlobalMsg(message);
				spawnAckRecThreads(coordinator);
			}*/
			
			// write end of transaction in Log file
			LOGGER.setLevel(Level.ALL);
			LOGGER.info("<<EOT>>");
			
//			System.out.println("Interaction over...");
			LOGGER.setLevel(Level.ALL);
			LOGGER.info("Interaction over...");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		catch (JMSException e)
		{
			e.printStackTrace();
		}
	}

	private void broadcastPrepareMsg() throws IOException, JMSException
	{
		// write begin commit to log

		// insert log code

		// send prepare message to the participants
		LOGGER.setLevel(Level.ALL);
		LOGGER.info("Sending <<"+SEND_PREPARE+">> to all");

		for (TransactionParticipant party : parties)
		{
			LOGGER.setLevel(Level.ALL);
			LOGGER.info("Sending <<"+SEND_PREPARE+">> to [" + party.getIpAddress() + ":" + party.getPortNo()+"]");
			
			Producer pro = new Producer();
			pro.CreateConnection(party.getIpAddress() + ":" + party.getPortNo());
			pro.SendObjectMessage(SEND_PREPARE,"");
			pro.CloseConnection();
		}
	}

	/*
	 *  Broadcast Global message either Global-Commit or Global-Abort
	 */
	private void sendGlobalMsg(String message) throws IOException, JMSException
	{
		LOGGER.setLevel(Level.ALL);
		LOGGER.info("Broadcasting <<"+message.toUpperCase()+">>");

		for(TransactionParticipant party:parties)
		{
			Producer pro = new Producer();
			pro.CreateConnection(party.getIpAddress() + ":" + party.getPortNo());
			pro.SendObjectMessage(message,"");
			pro.CloseConnection();

			LOGGER.setLevel(Level.ALL);
			LOGGER.info("<<"+message.toUpperCase()+">> sent to [" + party.getIpAddress() + ":" + party.getPortNo()+"]");
		}
	}
	
	/*private void resendGlobalMsg(String message) throws IOException, InterruptedException, JMSException
	{
		System.out.println("Resending " + message + " to participants who didn't ack...");

		for(Socket socket:partiesDontAck)
		{
			socket.setSoTimeout(TIMEOUT);
			socketList.add(socket);
			PrintWriter writer = new PrintWriter(socket.getOutputStream());
			Producer pro = new Producer();
			pro.CreateConnection(socket.getInetAddress().getHostAddress() + ":" + socket.getPort());
			pro.SendObjectMessage(message,"");
			pro.CloseConnection();

			writer.println(message); 
//			System.out.println("<<"+message.toUpperCase()+">> resent to [" + socket.getInetAddress().getHostAddress() + ":" + socket.getPort()+"]");
			LOGGER.setLevel(Level.ALL);
			LOGGER.info("<<"+message.toUpperCase()+">> resent to [" + socket.getInetAddress().getHostAddress() + ":" + socket.getPort()+"]");
			writer.flush();
		}
	}
*/	
	
	private void receiveVotes(TransactionParticipant coordinator) throws JMSException
	{
		Consumer consumer = new Consumer();
		consumer.CreateConnection(coordinator.getIpAddress() + ":" + coordinator.getPortNo(),"");
		System.out.println(":::"+coordinator.getIpAddress() + ":" + coordinator.getPortNo());
		
		for(int i=0;i<parties.size();++i)
		{
			StringBuffer msgFrom =new StringBuffer("");
			String resultMsg = (String)consumer.ReceiveMessage(msgFrom);
			System.out.println("RECVD : "+ resultMsg +" FROM " + msgFrom);
			
			if (resultMsg.equals(VOTE_COMMIT_RECIEVED))
			{
				
				LOGGER.setLevel(Level.ALL);
				LOGGER.info("Vote commit recieved from [" + msgFrom.toString() +"]");
			}
			else if(resultMsg.equalsIgnoreCase(VOTE_ABORT_RECIEVED))
			{
				LOGGER.setLevel(Level.WARNING);
				LOGGER.warning("Vote abort recieved from [" + msgFrom +"]");
				String[] pt = msgFrom.toString().split(":");
				TransactionParticipant party = new TransactionParticipant(-1,false,pt[0],Integer.parseInt(pt[1]));
				Coordinator.partiesVotedAbort.add(party);
				Coordinator.toCommit = false;
			}
		}
		consumer.CloseConnection();
	}
	
	@SuppressWarnings("deprecation")
	private void spawnsVoteRecThreads(TransactionParticipant coordinator) throws InterruptedException, JMSException
	{
		Consumer consumer = new Consumer();
		System.out.println(":::"+coordinator.getIpAddress() + ":" + coordinator.getPortNo());
		consumer.CreateConnection(coordinator.getIpAddress() + ":" + coordinator.getPortNo(),"");
		
		for(int i=0;i<parties.size();++i)
		{
			Runnable voteThread = new AskVotesFromParticipant(consumer,LOGGER); 
			Thread thread = new Thread(voteThread);
			thread.setName("participant"+(i+1));
			thread.start();
			threadList.add(thread);
		}
		
		long oldTime,currentTime;
		boolean continueWaiting;
		oldTime = System.currentTimeMillis();
		
		// check the threads after some 
		do
		{
			continueWaiting = false;
			for(Thread thread:threadList)
			{
				if(thread.isAlive())
				{
					continueWaiting = true;
					break;
				}
			}
			currentTime = System.currentTimeMillis();
		}while(continueWaiting && currentTime-oldTime < TIMEOUT);
		
		if(continueWaiting)	// time-out case
		{
			toCommit = false;
			
			// kill the thread
			for(Thread thread:threadList)
			{
				if(thread.isAlive())
				{
					thread.stop();
					break;
				}
			}
		}
		
		consumer.CloseConnection();
		
		// remove the parties who has aborted the transaction
		for(TransactionParticipant abortedParty:partiesVotedAbort)
			parties.remove(abortedParty);
		
		// join the threads
		
		/*if(continueWaiting == false)
		{
			for(Thread thread:threadList)
			{
				thread.join();
			}
		}*/
	}
	
	/*private void spawnAckRecThreads(TransactionParticipant coordinator) throws InterruptedException
	{
		for(int i=0;i<parties.size();++i)
		{
			Runnable recAck = new RecieveAckFromValidParty(coordinator,LOGGER);
			Thread thread = new Thread(recAck);
			thread.start();
			threadList.add(thread);
		}
		
		// join the threads
		for(Thread thread:threadList)
			thread.join();
	}*/
	
	private void recieveAck(TransactionParticipant coordinator) throws JMSException
	{
		Consumer consumer = new Consumer();
		consumer.CreateConnection(coordinator.getIpAddress() + ":" + coordinator.getPortNo(),"");
		System.out.println(">>>"+coordinator.getIpAddress() + ":" + coordinator.getPortNo());
		int cntAck = 0;
		
		int partySize = parties.size();
		
		while(true)
		{
//			System.out.println("CNTACK="+cntAck+"  parties= "+parties.size());
			if(cntAck >= partySize)
				break;
			StringBuffer msgFrom = new StringBuffer("");			
			String resMsg = (String)consumer.ReceiveMessage(msgFrom);
//			System.out.println("in Ack : " + resMsg + " from " + msgFrom);
			if(resMsg.equals(VOTE_ABORT_RECIEVED))
			{
				partySize--;
			}
			else if (resMsg.equals(ACK_RECIEVED))
			{
				System.out.println("RECVD : "+ resMsg +" FROM "+msgFrom);
				LOGGER.setLevel(Level.ALL);
				LOGGER.info("Recieved ACK from [" + msgFrom +"]");
				cntAck++;
			}
//			System.out.println("RECVD : "+ resMsg +" FROM "+msgFrom);
		}

		consumer.CloseConnection();
	}
}
