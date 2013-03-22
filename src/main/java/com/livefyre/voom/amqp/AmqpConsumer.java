package com.livefyre.voom.amqp;

import java.io.IOException;

import org.vertx.java.core.Handler;

import com.livefyre.voom.VoomMessage;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public abstract class AmqpConsumer extends DefaultConsumer {
    protected Handler<AmqpResponse> handler;

    public AmqpConsumer(Channel channel, Handler<AmqpResponse> handler) {
        super(channel);
        System.out.print(String.format("Bhandler=%s", handler));
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
                decode(body));

        handler.handle(resp);
    }

    public abstract VoomMessage decode(byte[] body) throws IOException;
    
    public class AmqpResponse {
        public String consumerTag;
        public Envelope envelope;
        public AMQP.BasicProperties properties;
        public VoomMessage body;
        
        public AmqpResponse(String consumerTag, Envelope envelope,
                BasicProperties properties, VoomMessage body) {
            super();
            this.consumerTag = consumerTag;
            this.envelope = envelope;
            this.properties = properties;
            this.body = body;
        }
    }
}
