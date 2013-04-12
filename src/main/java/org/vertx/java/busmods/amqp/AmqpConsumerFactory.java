package org.vertx.java.busmods.amqp;

import java.io.IOException;
import java.util.Map.Entry;

import org.vertx.java.busmods.amqp.AmqpConnectionManager.ConsumerFactory;

import com.google.protobuf.Message;
import com.livefyre.voom.VoomHeaders;
import com.livefyre.voom.VoomMessage;
import com.livefyre.voom.codec.MessageCodec;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class AmqpConsumerFactory implements ConsumerFactory {
	private MessageCodec<Message> codec;

	public AmqpConsumerFactory(MessageCodec<Message> codec) {
		super();
		this.codec = codec;
	}
	
	public Consumer newConsumer(Channel channel) {
		final AmqpConsumerFactory self = this;
		return new DefaultConsumer(channel) {
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
				self.handleDelivery(consumerTag, envelope, properties, body);
			}
		};
	}
	
	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
		
	}
	
	public VoomMessage<Message> decode(Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
		VoomMessage<Message> msg = codec.decodeMessage(body);
		msg.putHeaders(getHeaders(envelope, properties));
		return msg;
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
}


