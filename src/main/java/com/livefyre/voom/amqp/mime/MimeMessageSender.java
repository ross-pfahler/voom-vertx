package com.livefyre.voom.amqp.mime;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.mail.MessagingException;

import com.livefyre.voom.VoomMessage;
import com.livefyre.voom.amqp.AmqpSender;
import com.livefyre.voom.codec.mime.MimeEncoder;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

public class MimeMessageSender implements AmqpSender {
    private MimeEncoder encoder = new MimeEncoder();
    
    public void send(Channel channel, VoomMessage msg) throws IOException, MessagingException {
        Map<String, String> headers = new HashMap<String, String>();
        headers.putAll(msg.getHeaders().toMap());

        String exchange = headers.remove("exchange");
        if (exchange == null) {
            exchange = "";
        }

        String routingKey = headers.remove("routing_key");
        AMQP.BasicProperties props = buildProperties(headers);
        
        channel.basicPublish(exchange, routingKey, props, encoder.encodeMultipart(msg));
    }
    
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
}
