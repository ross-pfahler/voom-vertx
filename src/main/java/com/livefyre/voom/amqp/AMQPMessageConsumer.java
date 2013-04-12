package com.livefyre.voom.amqp;

import java.io.IOException;
import java.util.Map.Entry;

import org.vertx.java.core.Handler;

import com.livefyre.voom.VoomHeaders;
import com.livefyre.voom.VoomMessage;
import com.livefyre.voom.codec.MessageCodec;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class AMQPMessageConsumer<T> extends DefaultConsumer {
    protected Handler<AmqpResponse> handler;
    protected MessageCodec<T> codec;

    public AMQPMessageConsumer(Channel channel, MessageCodec<T> codec, Handler<AmqpResponse> handler) {
        this(channel, codec);
        this.handler = handler;
    }

    public AMQPMessageConsumer(Channel channel, MessageCodec<T> codec) {
        super(channel);
        this.codec = codec;
    }
    
    public AMQPMessageConsumer(Channel channel, AMQPMessageConsumer<T> other) {
    	this(channel, other.codec, other.handler);
    }
    
    public AMQPMessageConsumer<T> clone(Channel channel, AMQPMessageConsumer<T> other) {
    	return new AMQPMessageConsumer<T>(channel, other);
    }

    public void handler(Handler<AmqpResponse> handler) {
        this.handler = handler;
    }
    
    public void handleDelivery(final String consumerTag,
            final Envelope envelope,
            final AMQP.BasicProperties properties,
            final byte[] body)
                    throws IOException
                    {
        
        // TODO: We're only handling multipart/mixed because that's what
        // we're getting from the other side, handle other content types?
        
        AmqpResponse resp = new AmqpResponse(consumerTag, 
                envelope, 
                properties,
                codec.decodeMessage(body));
        
        resp.body.putHeaders(getHeaders(envelope, properties));
        
        handler.handle(resp);
    }
    
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
        
    public class AmqpResponse {
        public String consumerTag;
        public Envelope envelope;
        public AMQP.BasicProperties properties;
        public VoomMessage<T> body;
        
        public AmqpResponse(String consumerTag, Envelope envelope,
                BasicProperties properties, VoomMessage<T> body) {
            super();
            this.consumerTag = consumerTag;
            this.envelope = envelope;
            this.properties = properties;
            this.body = body;
        }
    }
}
