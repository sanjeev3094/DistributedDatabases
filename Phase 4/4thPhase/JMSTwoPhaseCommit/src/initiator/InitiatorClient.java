package initiator;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import pojo.TransactionParticipant;

public class InitiatorClient
{
	public static ArrayList<TransactionParticipant> participantList1 = new ArrayList<TransactionParticipant>();
	private final static Logger LOGGER = Logger.getLogger(InitiatorClient.class.getName());

	public static void fillParticipantDetails()
	{
		TransactionParticipant p1 = new TransactionParticipant();
		p1.setIpAddress(Constants.IPADD_P1);
		p1.setPortNo(Constants.PORT_P1);
		TransactionParticipant p2 = new TransactionParticipant();
		p2.setIpAddress(Constants.IPADD_P2);
		p2.setPortNo(Constants.PORT_P2);
		TransactionParticipant p3 = new TransactionParticipant();
		p3.setIpAddress(Constants.IPADD_P3);
		p3.setPortNo(Constants.PORT_P3);
		participantList1.add(p1);
		participantList1.add(p2);
		participantList1.add(p3);
	}
	
	public static void electCoordinator(ArrayList<TransactionParticipant> participantList)
	{
		// highest priority participant acts as Coordinator
		int maxPriority = Integer.MIN_VALUE;

		for (TransactionParticipant participant : participantList)
			if (participant.getPriority() > maxPriority)
				maxPriority = participant.getPriority();

//		System.out.println(" Max Priority " + maxPriority);
		LOGGER.setLevel(Level.ALL);
		LOGGER.info("Max Priority " + maxPriority);

		// send message to the participant
		for (TransactionParticipant participant : participantList)
			if(participant.getPriority() == maxPriority)
				participant.setCoordinator(true);
	}
	
	public static void main(String[] args)
	{
		fillParticipantDetails();
		ArrayList<TransactionParticipant> participantList = new ArrayList<TransactionParticipant>();
		ArrayList<Socket> socketList = new ArrayList<Socket>();
		
		try
		{
			for(TransactionParticipant participant:participantList1)
			{
				Socket initiatorSocket = new Socket(participant.getIpAddress(),participant.getPortNo());
				socketList.add(initiatorSocket);
				InputStream inputStream = initiatorSocket.getInputStream();
				ObjectInput objectInput = new ObjectInputStream(inputStream);
				Object obj = objectInput.readObject();
				TransactionParticipant part = (TransactionParticipant)obj;
//				System.out.println(part.getIpAddress() + " " + part.getPriority());
				LOGGER.setLevel(Level.ALL);
				LOGGER.info(part.getIpAddress() + " " + part.getPriority());

				participantList.add(part);
			}
			
			electCoordinator(participantList);
			
			// send the participantList to the participant servers
			for(int i=0;i<socketList.size();++i)
			{
				OutputStream outputStream = socketList.get(i).getOutputStream();
				ObjectOutput objectOutput = new ObjectOutputStream(outputStream);
				objectOutput.writeObject(participantList);
			}
		}
		catch (UnknownHostException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
//			System.err.println("IO Error...");
			LOGGER.setLevel(Level.ALL);
			LOGGER.severe("IO Error");
			e.printStackTrace();
		}
		catch (ClassNotFoundException e)
		{
			LOGGER.setLevel(Level.SEVERE);
			LOGGER.severe("ClassNotFoundException");
//			System.err.println("Wrong object read on the socket...");
			e.printStackTrace();
		}
	}

}
