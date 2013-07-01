package com.livefyre.voom.vertx;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import javax.mail.MessagingException;

import net.joshdevins.rabbitmq.client.ha.AbstractHaConnectionListener;
import net.joshdevins.rabbitmq.client.ha.HaConnectionFactory;
import net.joshdevins.rabbitmq.client.ha.HaConnectionProxy;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import com.livefyre.voom.ProtobufLoader;
import com.livefyre.voom.VoomHeaders;
import com.livefyre.voom.VoomMessage;
import com.livefyre.voom.amqp.AMQPMessageConsumer;
import com.livefyre.voom.amqp.AMQPMessageSender;
import com.livefyre.voom.codec.MessageCodec;
import com.livefyre.voom.codec.protobuf.MimeProtobufBinaryCodec;
import com.livefyre.voom.codec.protobuf.MimeProtobufMessageCodec;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;

public class VoomBridge extends BusModBase {
    private Connection conn;
    private Map<Long, Channel> consumerChannels = new HashMap<>();
    private Map<String, Channel> replyChannels = new HashMap<>();
    private long consumerSeq;
    private Queue<Channel> availableChannels = new LinkedList<>();
    private String defaultContentType;
    
    private MessageCodec<com.google.protobuf.Message> codec;
    
    // {{{ start
    /** {@inheritDoc} */
    @Override
    public void start() {
        super.start();
        
        logger.info("Starting VoomBridge.");
        
        final String address = getMandatoryStringConfig("address");
        String uri = getMandatoryStringConfig("uri");

        logger.trace("address: " + address);

        defaultContentType = getMandatoryStringConfig("defaultContentType");

        HaConnectionFactory factory = new HaConnectionFactory();
        factory.addHaConnectionListener(new ReconnectListener());
        
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
            conn = factory.newConnection(); // IOException
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create connection", e);
        }

        ProtobufLoader loader = new ProtobufLoader("com.livefyre.");
        codec = new MimeProtobufMessageCodec(new MimeProtobufBinaryCodec(loader));

        
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
        
        eb.registerHandler(address + ".send", new Handler<Message<VoomMessage<com.google.protobuf.Message>>>() {
            public void handle(final Message<VoomMessage<com.google.protobuf.Message>> message) {
                handleSend(message);
            }
        });

    }
    // }}}
    
    public void handleReconnect() throws IOException {
    //    consumerChannels.clear();
        //replyChannels.clear();
        
        for (Entry<String, Channel> entry:replyChannels.entrySet()) {
            entry.getValue().queueDeclare(entry.getKey(), false, true, true, null);
        }
        eb.publish("voom.reconnect", new JsonObject());
    }

    public void ensureReplyChannel(final String queueName) throws IOException {
        if (replyChannels.containsKey(queueName)) {
            return;
        }
        Channel channel = getChannel();
        AMQPMessageConsumer<com.google.protobuf.Message> cons = new AMQPMessageConsumer<com.google.protobuf.Message>(channel, codec) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                    BasicProperties properties, VoomMessage<com.google.protobuf.Message> msg) {
                VoomHeaders headers = msg.getHeaders();
                logger.info(String.format("Received response of type %s, replyTo=%s, correlationId=%s",
                        headers.contentType(), 
                        headers.replyTo(), 
                        headers.correlationId()));
                
                eb.send(queueName, msg);
                try {
                    getChannel().basicAck(envelope.getDeliveryTag(), false);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }                    
            }
        };

        logger.info(String.format("Registering reply queue, name=%s", queueName));
        channel.queueDeclare(queueName, false, true, true, null);
        logger.info(String.format("Declared queue"));
        channel.basicConsume(queueName, cons);
        logger.info(String.format("basic consume"));
        replyChannels.put(queueName, channel);
    }
    
    // {{{ stop
    /** {@inheritDoc} */
    @Override
    public void stop() {
        consumerChannels.clear();

        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                logger.warn("Failed to close", e);
            }
        }
    }
    // }}}

    // {{{ getChannel
    private Channel getChannel() throws IOException {
        if (! availableChannels.isEmpty()) {
            return availableChannels.remove();
        } else {
            return conn.createChannel(); // IOException
        }
    }
    // }}}

    // {{{ send
    private void send(final AMQP.BasicProperties _props, final VoomMessage<com.google.protobuf.Message> message)
        throws IOException, MessagingException
    {
        logger.info("MEEEOW");
        Channel channel = getChannel();
        availableChannels.add(channel);
        VoomHeaders headers = message.getHeaders();
        String ctype = headers.contentType().getBaseType();
        logger.debug(String.format("Sending message of type %s, replyTo=%s, correlationId=%s",
                ctype, headers.replyTo(), headers.correlationId()));
        if (headers.replyTo() != null) {
            ensureReplyChannel(headers.replyTo());
        }

        (new AMQPMessageSender<com.google.protobuf.Message>(
                codec)).send(channel, message);
    }
    // }}}

    // {{{ createConsumer
    private long createConsumer(final String exchangeName,
                                final String routingKey,
                                final String forwardAddress,
                                final String contentType)
        throws IOException
    {
        Channel channel = getChannel();
        AMQPMessageConsumer<com.google.protobuf.Message> cons = new AMQPMessageConsumer<com.google.protobuf.Message>(channel, codec) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                    BasicProperties properties, VoomMessage<com.google.protobuf.Message> msg) {
                eb.send(forwardAddress, msg);                    
            }
        };
        
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, exchangeName, routingKey);
        channel.basicConsume(queueName, cons);        
        
        long id = consumerSeq++;
        consumerChannels.put(id, channel);

        return id;
    }
    // }}}

    // {{{ closeConsumer
    private void closeConsumer(final long id) {
        Channel channel = consumerChannels.remove(id);

        if (channel != null) {
            availableChannels.add(channel);
        }
    }
    // }}}

    // {{{ handleCreateConsumer
    private void handleCreateConsumer(final Message<JsonObject> message) {
        String exchange = message.body.getString("exchange", "");
        String routingKey = message.body.getString("routingKey");
        String forwardAddress = message.body.getString("forward");
        String contentType = message.body.getString("content-type", defaultContentType);

        JsonObject reply = new JsonObject();

        try {
            reply.putNumber("id", createConsumer(exchange, routingKey, forwardAddress, contentType));

            //sendOK(message, reply);
        } catch (IOException e) {
            //sendError(message, "unable to create consumer: " + e.getMessage(), e);
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
    private void handleSend(final Message<VoomMessage<com.google.protobuf.Message>> message) {
        try {
            send(null, message.body);
            // TODO
            // sendOK(message);
        } catch (IOException | MessagingException e) {
            e.printStackTrace();
            // TODO
            // sendError(message, "unable to send: " + e.getMessage(), e);
        }
    }
    // }}}
    
    private class ReconnectListener extends AbstractHaConnectionListener {
        public void onReconnection(final HaConnectionProxy connectionProxy) {
            try {
                handleReconnect();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
