package org.vertx.java.busmods.amqp;

import com.livefyre.voom.ProtobufLoader;
import com.livefyre.voom.VoomHeaders;
import com.livefyre.voom.VoomMessage;
import com.livefyre.voom.codec.MessageCodec;
import com.livefyre.voom.codec.protobuf.MimeProtobufBinaryCodec;
import com.livefyre.voom.codec.protobuf.MimeProtobufMessageCodec;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.busmods.amqp.AmqpConnectionManager.AmqpConnectionReset;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;

import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.KeyManagementException;

import java.net.URISyntaxException;

import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import javax.mail.MessagingException;

/**
 * Prototype for AMQP bridge
 * Currently only does pub/sub and does not declare exchanges so only works with default exchanges
 * Three operations:
 * 1) Create a consumer on a topic given exchange name (use amqp.topic) and routing key (topic name)
 * 2) Close a consumer
 * 3) Send message
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 * @author <a href="http://github.com/blalor">Brian Lalor</a>
 */
public class AmqpBridge extends BusModBase {
    private AmqpConnectionManager conn;
    private String defaultContentType;
    private Set<String> replyChannels = new HashSet<String>();
    
    private Map<String, MessageCodec<com.google.protobuf.Message>> codecs = new HashMap<String, MessageCodec<com.google.protobuf.Message>>();
	private String address;

    // {{{ start
    /** {@inheritDoc} */
    @Override
    public void start() {
        super.start();
        
        address = getMandatoryStringConfig("address");
        String uri = getMandatoryStringConfig("uri");

        logger.trace("address: " + address);

        defaultContentType = getMandatoryStringConfig("defaultContentType");

        ConnectionFactory factory = new ConnectionFactory();

        try {
            factory.setUri(uri);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("illegal uri: " + uri, e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("illegal uri: " + uri, e);
        } catch (KeyManagementException e) {
            throw new IllegalArgumentException("illegal uri: " + uri, e);
        }

        try {
            conn = new AmqpConnectionManager(factory); // IOException
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create connection", e);
        }

        registerContentTypes();
        
//        try {
//            rpcCallbackHandler = new RPCCallbackHandler(getChannel(), defaultContentType, eb);
//        } catch (IOException e) {
//            throw new IllegalStateException("Unable to create queue for callbacks", e);
//        }

        // register handlers
        eb.registerHandler(address + ".create-consumer", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> message) {
                handleCreateConsumer(message);
            }
        });

        eb.registerHandler(address + ".close-consumer", new Handler<Message<JsonObject>>() {
            public void handle(final Message<JsonObject> message) {
                handleCloseConsumer(message);
            }
        });
        
        eb.registerHandler(address + ".send", new Handler<Message<VoomMessage>>() {
            public void handle(final Message<VoomMessage> message) {
                handleSend(message);
            }
        });

//        eb.registerHandler(address + ".invoke_rpc", new Handler<Message<JsonObject>>() {
//            public void handle(final Message<JsonObject> message) {
//                handleInvokeRPC(message);
//            }
//        });
    }
    // }}}
    
    public void ensureReplyChannel(final String queueName) throws IOException {
        if (replyChannels.contains(queueName)) {
            return;
        }

        AmqpConsumerFactory factory = new AmqpConsumerFactory(codecs.get("multipart/mixed")) {
        	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        		VoomMessage<com.google.protobuf.Message> msg = decode(envelope, properties, body);
                VoomHeaders headers = msg.getHeaders();
                logger.info(String.format("Received response of type %s, replyTo=%s, correlationId=%s",
                        headers.contentType(), 
                        headers.replyTo(), 
                        headers.correlationId()));
                
                eb.send(queueName, msg);
                try {
                    conn.basicAck(envelope.getDeliveryTag(), false);
                } catch (AmqpConnectionReset resetError) {
                	sendConnectionReset();
                }
                catch (IOException e) {
                    // TODO Auto-generated catch block
                    sendConnectionError();
                }

        	}
        };

        logger.info(String.format("Registering reply queue, name=%s", queueName));
        conn.queueDeclare(queueName, false, true, true, null);
        conn.basicConsume(queueName, factory);
        replyChannels.add(queueName);
    }
        
    public void registerContentTypes() {
        ProtobufLoader loader = new ProtobufLoader("com.livefyre.");
        codecs.put("multipart/mixed", new MimeProtobufMessageCodec(
                new MimeProtobufBinaryCodec(loader)));
    }
    
    // {{{ stop
    /** {@inheritDoc} */
    @Override
    public void stop() {
    	replyChannels.clear();

        if (conn != null) {
            try {
                conn.disconnect();
            } catch (Exception e) {
                logger.warn("Failed to close", e);
            }
        }
    }
    // }}}

    // {{{ send
    private void send(final AMQP.BasicProperties _props, final VoomMessage<com.google.protobuf.Message> message)
        throws IOException, MessagingException
    {
        String ctype = message.getHeaders().contentType().getBaseType();
        logger.info(String.format("Sending message of type %s, replyTo=%s, correlationId=%s",
                ctype, message.getHeaders().replyTo(), message.getHeaders().correlationId()));

        if (message.getHeaders().replyTo() != null) {
        	ensureReplyChannel(message.getHeaders().replyTo());
        }

        Map<String, String> headers = new HashMap<String, String>();
        headers.putAll(message.getHeaders().toMap());

        String exchange = headers.remove("exchange");
        if (exchange == null) {
            exchange = "";
        }

        String routingKey = headers.remove("routing_key");
        AMQP.BasicProperties props = buildProperties(headers);
        
        conn.basicPublish(exchange, routingKey, props, 
        		codecs.get("multipart/mixed").encodeMessage(message));
    }
    // }}}
    
    private AMQP.BasicProperties buildProperties(Map<String, String> headers) {
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        AMQP.BasicProperties.Builder builder = props.builder();

        builder.clusterId(headers.remove("cluster_id"));
        builder.contentType(headers.remove("content-type"));
        builder.contentEncoding(headers.remove("content-encoding"));
        builder.replyTo(headers.remove("reply_to"));
        builder.correlationId(headers.remove("correlation_id"));
        
        if (headers.containsKey("delivery_mode")) {
            builder.deliveryMode(Integer.parseInt(headers.remove("delivery_mode")));
        }

        builder.expiration(headers.remove("expiration"));

        builder.messageId(headers.remove("message_id"));

        if (headers.containsKey("priority")) {
            builder.priority(Integer.parseInt(headers.remove("priority")));
        }

            // amqpPropsBuilder.timestamp(ebProps.getString("timestamp")); // @todo
        builder.type(headers.remove("type"));
        builder.userId(headers.remove("user_id"));
        
        Map<String, Object> objectHeaders = new HashMap<String, Object>();
        objectHeaders.putAll(headers);
        builder.headers(objectHeaders);
        return builder.build();
    }    
    
    // {{{ createConsumer
    private long createConsumer(final String exchangeName,
                                final String routingKey,
                                final String forwardAddress,
                                final String contentType)
        throws IOException
    {

    	AmqpConsumerFactory consumerFactory = new AmqpConsumerFactory(codecs.get("multipart/mixed")) {
    		public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
    			VoomMessage<com.google.protobuf.Message> msg = decode(envelope, properties, body);
    			eb.send(forwardAddress, msg);
    			conn.basicAck(envelope.getDeliveryTag(), false);
    		}
    	};
    	
    	return conn.basicConsume(conn.queueDeclare().getQueue(), exchangeName, routingKey, consumerFactory);
    }
    // }}}

    public VoomHeaders getHeaders(Envelope envelope, AMQP.BasicProperties props) {
        VoomHeaders headers = new VoomHeaders();
        
        headers.put("cluster_id", props.getClusterId());
        headers.put("Content-Type", props.getContentType());
        headers.put("Content-Encoding", props.getContentEncoding());
        headers.put("reply_to", props.getReplyTo());
        headers.put("correlation_id", props.getCorrelationId());
        headers.putInt("delivery_mode", props.getDeliveryMode());
        headers.put("expiration", props.getExpiration());
        headers.put("message_id", props.getMessageId());
        headers.putInt("priority", props.getPriority());
        headers.put("type", props.getType());
        headers.put("user_id", props.getUserId());
        
        for (Entry<String, Object> header: props.getHeaders().entrySet()) {
            // TODO: How do we handle non-string values;
            headers.put(header.getKey(), header.getValue().toString());
        }
        
        headers.put("routing_key", envelope.getRoutingKey());
        headers.put("exchange", envelope.getExchange());
        
        return headers;
    }
        
    // {{{ closeConsumer
    private void closeConsumer(final long id) {
        conn.stopConsumer(id);
    }
    // }}}
    
    private void sendConnectionError() {
    	System.out.print("ERROROROR");
    	JsonObject errorMsg = new JsonObject();
    	errorMsg.putString("type", "ConnectionError");
    	eb.send(new StringBuilder(address).append(".error").toString(), errorMsg);
	}

	private void sendConnectionReset() {
		JsonObject errorMsg = new JsonObject();
    	errorMsg.putString("type", "ConnectionReset");
    	eb.send(new StringBuilder(address).append(".error").toString(), errorMsg);
	}

    // {{{ handleCreateConsumer
    private void handleCreateConsumer(final Message<JsonObject> message) {
        String exchange = message.body.getString("exchange", "");
        String routingKey = message.body.getString("routingKey");
        String forwardAddress = message.body.getString("forward");
        String contentType = message.body.getString("content-type", defaultContentType);

        JsonObject reply = new JsonObject();

        try {
            reply.putNumber("id", createConsumer(exchange, routingKey, forwardAddress, contentType));
        } catch (AmqpConnectionReset resetError) {
        	resetError.printStackTrace();
        	replyChannels.clear();
        	sendConnectionReset();
        }
        catch (IOException e) {
        	e.printStackTrace();
            sendConnectionError();
        }
    }
    // }}}

	// {{{ handleCloseConsumer
    private void handleCloseConsumer(final Message<JsonObject> message) {
        long id = (Long) message.body.getNumber("id");
    	closeConsumer(id);
    }
    // }}}

    // {{{ handleSend
    private <T> void handleSend(final Message<VoomMessage> message) {
        try {
            send(null, (VoomMessage<com.google.protobuf.Message>)message.body);
        } catch (AmqpConnectionReset resetError) {
        	resetError.printStackTrace();
        	replyChannels.clear();
        	sendConnectionReset();
        }
        catch (IOException e) {
        	e.printStackTrace();
            sendConnectionError();
        } catch (MessagingException e) {
			e.printStackTrace();
		}
    }
    // }}}
}
