package org.vertx.java.busmods.amqp;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ShutdownSignalException;

public class AmqpConnectionManager {
	static int DEFAULT_RETRIES = 3;
	private int nRetries;
	private ConnectionFactory connectionFactory;
	private Connection connection;
    private Queue<Channel> availableChannels = new LinkedList<>();
    private Map<Long, AmqpConsumerDetails> consumerChannels = new HashMap<>();
    private long consumerSeq;
    
	public AmqpConnectionManager(ConnectionFactory connectionFactory) throws IOException {
		super();
		this.connectionFactory = connectionFactory;
		this.connection = this.connectionFactory.newConnection();
		this.nRetries = DEFAULT_RETRIES;
	}

    private Channel getChannel(boolean force) throws IOException {
    	Channel channel;
    	System.out.print("Getting channel, size of availableChannels is " + availableChannels.size());
        if (!availableChannels.isEmpty() && !force) {
            channel = availableChannels.remove();
        } else {
        	try {
        		channel = connection.createChannel(); // IOException
        	} catch (ShutdownSignalException e) {
        		attemptReconnect();
        		throw new AmqpConnectionReset();
        	}
        }
        return channel;
    }
    
    private Channel getChannel() throws IOException {
    	return getChannel(false);
    }
    
    public AMQP.Queue.DeclareOk queueDeclare() throws IOException {
    	Channel ch = getChannel();
    	AMQP.Queue.DeclareOk res;
    	try {
    		res = ch.queueDeclare();
    	} catch(ShutdownSignalException e) {
			ch = getChannel(true);
    		res = ch.queueDeclare();    		
    	}
    	availableChannels.add(ch);
    	return res;
    }

    public AMQP.Queue.DeclareOk queueDeclare(java.lang.String queue, boolean durable, boolean exclusive, boolean autoDelete, java.util.Map<java.lang.String,java.lang.Object> arguments) throws IOException {
    	Channel ch = getChannel();
    	AMQP.Queue.DeclareOk res;
    	try {
    		res = ch.queueDeclare(queue, durable, exclusive, autoDelete, arguments);
    	} catch(ShutdownSignalException e) {
    		ch = getChannel(true);
    		res = ch.queueDeclare(queue, durable, exclusive, autoDelete, arguments);
    	}
    	availableChannels.add(ch);
    	return res;    	
    }
    
    public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) throws IOException {
    	Channel ch = getChannel();
    	try {
    		ch.basicPublish(exchange, routingKey, props, body);
    	} catch (ShutdownSignalException e) {
    		ch = getChannel(true);
    		ch.basicPublish(exchange, routingKey, props, body);
    	}
    	availableChannels.add(ch);
    }
    
    public void basicAck(long deliveryTag, boolean multiple) throws IOException {
    	Channel ch = getChannel();
    	try {
			ch.basicAck(deliveryTag, multiple);
		} catch (ShutdownSignalException e) {
			ch = getChannel(true);
			ch.basicAck(deliveryTag, multiple);
		}
    	availableChannels.add(ch);
    }
    
    public long basicConsume(String queueName, String exchangeName, String routingKey, ConsumerFactory consumerFactory) throws IOException {
    	AmqpConsumerDetails details = new AmqpConsumerDetails();
    	details.exchangeName = exchangeName;
    	details.routingKey = routingKey;
    	details.queueName = queueName;
    	details.channel = getChannel(true);
    	details.consumerFactory = consumerFactory;
    	details.id = consumerSeq++;
    	
    	try {
    		return consume(details);
    	} catch (ShutdownSignalException e) {
    		attemptReconnect();
    		throw new AmqpConnectionReset();
    	}
    }
    
	public void basicConsume(String queueName, ConsumerFactory consumerFactory) throws IOException {
		basicConsume(queueName, null, null, consumerFactory);
	}
    
    private long consume(AmqpConsumerDetails details) throws IOException {
    	consumerChannels.put(details.id, details);
    	if (details.exchangeName != null && details.exchangeName != "") {
    		details.channel.queueBind(details.queueName, details.exchangeName, details.routingKey);
    	}
    	details.channel.basicConsume(details.queueName, 
    			details.consumerFactory.newConsumer(details.channel));
        return details.id;
    }
    
    public void stopConsumer(long id) {
    	consumerChannels.remove(id);
    }
    
    private void connect() throws IOException {
    	int failures = 0;
    	while (true) {
    		try {
    			connection = this.connectionFactory.newConnection();
    			return;
    		} catch (IOException e) {
    			if (failures == nRetries) {
    				throw e;
    			}
    			failures++;
    			try {
					Thread.sleep(500);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
    		}
    		
    	}
    }
    
    public void disconnect() throws IOException {
        availableChannels.clear();
        consumerChannels.clear();
        try {
        	connection.close();
        } catch (ShutdownSignalException e) {
        	
        }
    }
    
    private void attemptReconnect() throws IOException {
    	disconnect();
    	connect();
    }
    
    private class AmqpConsumerDetails {
    	public String exchangeName;
        public String routingKey;
        public String queueName;
        public Channel channel;
        public ConsumerFactory consumerFactory;
        long id;
    }
    
    public interface ConsumerFactory {
    	public Consumer newConsumer(Channel channel);
    }
    
    public class AmqpConnectionReset extends IOException {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
    	
    }
}
