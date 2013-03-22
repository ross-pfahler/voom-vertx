package org.vertx.java.busmods.amqp;

import com.livefyre.voom.VoomHeaders;
import com.livefyre.voom.VoomMessage;
import com.livefyre.voom.amqp.AmqpAdapterRegistry;
import com.livefyre.voom.amqp.AmqpConsumer;
import com.livefyre.voom.amqp.mime.MimeMessageConsumer;
import com.livefyre.voom.amqp.mime.MimeMessageSender;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;

import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.KeyManagementException;

import java.net.URISyntaxException;

import java.util.Map;
import java.util.HashMap;
import java.util.Queue;
import java.util.LinkedList;

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
    private Connection conn;
    private Map<Long, Channel> consumerChannels = new HashMap<>();
    private Map<String, Channel> replyChannels = new HashMap<>();
    private long consumerSeq;
    private Queue<Channel> availableChannels = new LinkedList<>();

    private String defaultContentType;

    // {{{ start
    /** {@inheritDoc} */
    @Override
    public void start() {
        super.start();
        
        final String address = getMandatoryStringConfig("address");
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
            conn = factory.newConnection(); // IOException
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
                logger.info("Do'in work.");
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

    public void ensureReplyChannel(final String queueName, String contentType) throws IOException {
        logger.info("C");
        if (replyChannels.containsKey(queueName)) {
            return;
        }
        logger.info("D");
        Channel channel = getChannel();
        
        Consumer cons;
        try {
            cons = AmqpAdapterRegistry.getConsumer(contentType, channel, 
                    new Handler<AmqpConsumer.AmqpResponse>() {
                        public void handle(AmqpConsumer.AmqpResponse msg) {
                            VoomHeaders headers = msg.body.getHeaders();
                            logger.info(String.format("Received response of type %s, replyTo=%s, correlationId=%s",
                                    headers.contentType(), 
                                    headers.replyTo(), 
                                    headers.correlationId()));

                            eb.send(queueName, msg.body);
                            try {
                                getChannel().basicAck(msg.envelope.getDeliveryTag(), false);
                            } catch (IOException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
//                        long deliveryTag = envelope.getDeliveryTag();
//                        eb.send(forwardAddress, body);
//                        
                        }
            });
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            throw new IOException();
        }

        logger.info(String.format("Registering reply queue, name=%s", queueName));
        channel.queueDeclare(queueName, false, true, true, null);
        channel.basicConsume(queueName, cons);
        replyChannels.put(queueName, channel);
    }
    
    public void registerContentTypes() {
        AmqpAdapterRegistry.register("multipart/mixed", new MimeMessageSender());
        AmqpAdapterRegistry.register("multipart/mixed", MimeMessageConsumer.class);
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
    private void send(final AMQP.BasicProperties _props, final VoomMessage message)
        throws IOException, MessagingException
    {
        Channel channel = getChannel();
        availableChannels.add(channel);
        VoomHeaders headers = message.getHeaders();
        String ctype = headers.contentType().getBaseType();
        logger.info(String.format("Sending message of type %s, replyTo=%s, correlationId=%s",
                ctype, headers.replyTo(), headers.correlationId()));
        if (headers.replyTo() != null) {
            ensureReplyChannel(headers.replyTo(), ctype);
        }
        
        AmqpAdapterRegistry.getSender(ctype).send(channel, message);
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
        
        Consumer cons;
        try {
            cons = AmqpAdapterRegistry.getConsumer(contentType, channel, 
                    new Handler<AmqpConsumer.AmqpResponse>() {
                        public void handle(AmqpConsumer.AmqpResponse msg) {
                            eb.send(forwardAddress, msg.body);
//                        long deliveryTag = envelope.getDeliveryTag();
//                        eb.send(forwardAddress, body);
//                        getChannel().basicAck(deliveryTag, false);
                        }
            });
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            throw new IOException();
        }
        
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
    private <T> void handleSend(final Message<VoomMessage> message) {
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

    // {{{ handleInvokeRPC
//    private void handleInvokeRPC(final Message<JsonObject> message) {
//        // if replyTo is non-null, then this is a multiple-response RPC invocation
//        String replyTo = message.body.getString("replyTo");
//
//        boolean isMultiResponse = (replyTo != null);
//
//        // the correlationId is what ties this all together.
//        String correlationId = UUID.randomUUID().toString();
//
//        AMQP.BasicProperties.Builder amqpPropsBuilder = new AMQP.BasicProperties.Builder()
//            .correlationId(correlationId)
//            .replyTo(rpcCallbackHandler.getQueueName());
//
//        if (isMultiResponse) {
//            // multiple-response invocation
//
//            JsonObject msgProps = message.body.getObject("properties");
//
//            String ebCorrelationId = null;
//            Integer ttl = null;
//
//            if (msgProps != null) {
//                ebCorrelationId = msgProps.getString("correlationId");
//                ttl = ((Integer) msgProps.getNumber("timeToLive", 0)).intValue();
//
//                if (ttl == 0) {
//                    ttl = null;
//                }
//
//                // do not pass these on to send(); that could confuse things
//                msgProps.removeField("correlationId");
//                msgProps.removeField("timeToLive");
//            }
//
//            rpcCallbackHandler.addMultiResponseCorrelation(
//                correlationId,
//                ebCorrelationId,
//                replyTo,
//                ttl
//            );
//        } else {
//            // standard call/response invocation; message.reply() will be called
//            rpcCallbackHandler.addCorrelation(correlationId, message);
//        }
//
//        try {
//            send(amqpPropsBuilder.build(), message.body);
//
//            // always invoke message.reply to avoid ambiguity
//            if (isMultiResponse) {
//                sendOK(message);
//            }
//        } catch (IOException e) {
//            sendError(message, "unable to publish: " + e.getMessage(), e);
//        }
//    }
    // }}}
}
