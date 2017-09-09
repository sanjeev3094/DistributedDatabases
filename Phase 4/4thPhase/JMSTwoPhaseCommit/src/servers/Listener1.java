package servers;

import initiator.Constants;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import logging.MyLogger;

import participants.Coordinator;
import participants.Participant;
import pojo.TransactionParticipant;

public class Listener1
{
	public static int priority = 1;
	private final static Logger LOGGER = Logger.getLogger(Listener1.class.getName());

	@SuppressWarnings("unchecked")
	public static void main(String args[])
	{
		//UseLogger logger = new UseLogger();
		try 
		{
			MyLogger.setup1();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
			throw new RuntimeException("Problems with creating the log files");
		}
		//logger.writeLog();

		try
		{
			TransactionParticipant participant1 = new TransactionParticipant(priority, false, Constants.IPADD_P1, Constants.PORT_P1);

			ServerSocket server = new ServerSocket(Constants.PORT_P1);

			//System.out.println("Node 1 started at ["+Constants.IPADD_P1+":"+Constants.PORT_P1+"]");
			LOGGER.setLevel(Level.ALL);
			LOGGER.info("Node 1 started at ["+Constants.IPADD_P1+":"+Constants.PORT_P1+"]");

			ArrayList<TransactionParticipant> participantList = null;

//			while (true)
			{
				Socket socket = server.accept();
				ObjectOutput objectOutput = new ObjectOutputStream(socket.getOutputStream());
				objectOutput.writeObject(participant1);

				ObjectInput objectInput = new ObjectInputStream(socket.getInputStream());
				Object obj = objectInput.readObject();
				participantList = (ArrayList<TransactionParticipant>) obj;

				socket.close();
				//System.out.println("Interaction with initiator over...\n");
				LOGGER.setLevel(Level.ALL);
				LOGGER.info("Interaction with initiator over...");
				
				TransactionParticipant coordinator = null;
				for (TransactionParticipant partObj : participantList)
				{
					// //System.out.println(party);

					if (partObj.isCoordinator())
						coordinator = partObj;
				}

				participantList.remove(participant1);
				
				printParticipantList(participantList);
				
				if (coordinator != null && participant1.equals(coordinator))
				{
					//System.out.println("\nNode 1 acting as a coordinator...");
					LOGGER.setLevel(Level.ALL);
					LOGGER.info("Node 1 acting as a coordinator...");
					
					Coordinator coObj = new Coordinator();
					coObj.init(coordinator, participantList,LOGGER);
				}
				else
				{
					//System.out.println("\nNode 1 acting as a participant...");
					LOGGER.setLevel(Level.ALL);
					LOGGER.info("Node 1 acting as a participant...");
					Participant participantObj = new Participant();
					participantObj.init(coordinator, participantList, server,participant1, LOGGER);
				}
			}
			server.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
		}
	}
	
	static void printParticipantList(ArrayList<TransactionParticipant> participantList)
	{
		//System.out.println("Details of participant list on machine : " + Constants.IPADD_P1 + Constants.PORT_P1);
		LOGGER.setLevel(Level.ALL);
		LOGGER.info("Details of participant list on machine : [" + Constants.IPADD_P1 +":"+ Constants.PORT_P1+"]");
		
		for(TransactionParticipant participant:participantList)
		{
			//System.out.println(participant.getIpAddress() + " " + participant.getPortNo() + " " + participant.getPriority());
			LOGGER.setLevel(Level.ALL);
			LOGGER.info(participant.getIpAddress() + " " + participant.getPortNo() + " " + participant.getPriority());
		}
	}
}
