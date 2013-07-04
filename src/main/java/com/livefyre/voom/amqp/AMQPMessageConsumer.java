package com.livefyre.voom.amqp;

import java.io.IOException;
import java.util.Map.Entry;

import com.livefyre.voom.VoomHeaders;
import com.livefyre.voom.VoomMessage;
import com.livefyre.voom.codec.MessageCodec;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class AMQPMessageConsumer<T> extends DefaultConsumer {
    protected MessageCodec<T> codec;

    public AMQPMessageConsumer(Channel channel, MessageCodec<T> codec) {
        super(channel);
        this.codec = codec;
    }
    
    public void handleDelivery(final String consumerTag,
            final Envelope envelope,
            final AMQP.BasicProperties properties,
            final byte[] body)
                    throws IOException
                    {
        VoomMessage<T> msg = codec.decodeMessage(body);
        msg.putHeaders(getHeaders(envelope, properties));
        handleDelivery(consumerTag, envelope, properties, msg);
    }
    
    protected void handleDelivery(final String consumerTag,
            final Envelope envelope,
            final AMQP.BasicProperties properties,
            final VoomMessage<T> body)
                    throws IOException
                    {
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
            if (header.getValue() != null) {
                headers.put(header.getKey(), header.getValue().toString());
            }
        }
        
        headers.put("routing_key", envelope.getRoutingKey());
        headers.put("exchange", envelope.getExchange());
        
        return headers;
    }
}
