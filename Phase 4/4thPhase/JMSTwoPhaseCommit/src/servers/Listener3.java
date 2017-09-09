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
//import logging.UseLogger;

import participants.Coordinator;
import participants.Participant;
import pojo.TransactionParticipant;

public class Listener3
{
	public static int priority = 3;
	private final static Logger LOGGER = Logger.getLogger(Listener2.class.getName());

	@SuppressWarnings("unchecked")
	public static void main(String args[])
	{
//		UseLogger logger = new UseLogger();
		try 
		{
			MyLogger.setup3();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
			throw new RuntimeException("Problems with creating the log files");
		}
//		logger.writeLog();
		try
		{
			TransactionParticipant participant3 = new TransactionParticipant(priority, false, Constants.IPADD_P3, Constants.PORT_P3);

			ServerSocket server = new ServerSocket(Constants.PORT_P3);

			//System.out.println("Node 3 started at ["+Constants.IPADD_P3+":"+Constants.PORT_P3+"]");
			LOGGER.setLevel(Level.ALL);
			LOGGER.info("Node 3 started at ["+Constants.IPADD_P3+":"+Constants.PORT_P3+"]");
			ArrayList<TransactionParticipant> participantList = null;

//			while (true)
			{
				Socket socket = server.accept();
				ObjectOutput objectOutput = new ObjectOutputStream(socket.getOutputStream());
				objectOutput.writeObject(participant3);

				ObjectInput objectInput = new ObjectInputStream(socket.getInputStream());
				Object obj = objectInput.readObject();
				participantList = (ArrayList<TransactionParticipant>) obj;

				socket.close();

				TransactionParticipant coordinator = null;
				for (TransactionParticipant partObj : participantList)
				{
					// //System.out.println(party);

					if (partObj.isCoordinator())
						coordinator = partObj;
				}

				participantList.remove(participant3);
				printParticipantList(participantList);
				//System.out.println("Interaction with initiator over...\n");
				LOGGER.setLevel(Level.ALL);
				LOGGER.info("Interaction with initiator over...");
				
				if (coordinator != null && participant3.equals(coordinator))
				{
					Coordinator coObj = new Coordinator();
					//System.out.println("\nNode 3 acting as a coordinator...");
					LOGGER.setLevel(Level.ALL);
					LOGGER.info("Node 3 acting as a coordinator...");
					coObj.init(coordinator, participantList,LOGGER);
				}
				else
				{
					Participant participantObj = new Participant();
					//System.out.println("\nNode 3 acting as a participant...");
					LOGGER.setLevel(Level.ALL);
					LOGGER.info("Node 3 acting as a participant...");
					participantObj.init(coordinator, participantList, server, participant3, LOGGER);
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
		//System.out.println("Details of participant list on machine : " + Constants.IPADD_P3 + Constants.PORT_P3);
		LOGGER.setLevel(Level.ALL);
		LOGGER.info("Details of participant list on machine : [" + Constants.IPADD_P3 +":" +Constants.PORT_P3+"]");
		
		for(TransactionParticipant participant:participantList)
		{
			//System.out.println(participant.getIpAddress() + " " + participant.getPortNo() + " " + participant.getPriority());
			LOGGER.setLevel(Level.ALL);
			LOGGER.info(participant.getIpAddress() + " " + participant.getPortNo() + " " + participant.getPriority());
		}
	}
}
