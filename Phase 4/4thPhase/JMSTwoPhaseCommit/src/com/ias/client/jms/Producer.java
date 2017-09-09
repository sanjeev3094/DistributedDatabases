package com.ias.client.jms;

import java.io.Serializable;

import javax.jms.*;

import org.apache.activemq.ActiveMQConnectionFactory;

public class Producer
{
	// URL of the JMS server. DEFAULT_BROKER_URL will just mean
	// that JMS server is on localhost
	private static String url = "failover://tcp://10.3.3.119:61616";

	public Connection connection;
	public Session session;
	public Queue destination;
	public MessageProducer producer;

	public void CreateConnection(String queueName) throws JMSException
	{
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
		connection = connectionFactory.createConnection();
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		this.destination = session.createQueue(queueName);
		producer = session.createProducer(this.destination);
	}

	public void CloseConnection() throws JMSException
	{
		connection.close();
	}

	public void SendObjectMessage(Serializable message, String messageSelector) throws JMSException
	{
		{
			ObjectMessage objMessage = session.createObjectMessage();
			objMessage.setJMSType(messageSelector);
			objMessage.setObject(message);
			producer.send(objMessage);
		}

	}

}
