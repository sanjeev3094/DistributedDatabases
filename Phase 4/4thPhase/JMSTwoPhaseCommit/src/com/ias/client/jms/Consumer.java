package com.ias.client.jms;

import java.io.Serializable;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

public class Consumer
{
	private static String url = "failover://tcp://10.3.3.119:61616";

	public Connection connection;
	public Session session;
	public Queue destination;
	public MessageConsumer consumer;

	public void CreateConnection(String queue, String messageSelector) throws JMSException
	{
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
		connection = connectionFactory.createConnection();
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		this.destination = session.createQueue(queue);
		if (messageSelector.isEmpty())
			consumer = session.createConsumer(this.destination);
		else
		{
			messageSelector = "JMSType = '" + messageSelector + "'";
			consumer = session.createConsumer(this.destination);
		}
	}

	public Serializable ReceiveMessage(StringBuffer jmsType) throws JMSException
	{
		ObjectMessage message = (ObjectMessage) consumer.receive();
		jmsType.append(message.getJMSType());
		System.out.println("In consumer : " + jmsType);
		return message.getObject();
	}
	public Serializable ReceiveMessage() throws JMSException
	{
		ObjectMessage message = (ObjectMessage) consumer.receive();
		return message.getObject();
	}

	public void CloseConnection() throws JMSException
	{
		connection.close();
	}

}