package com.livefyre.voom.amqp;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.vertx.java.core.Handler;

import com.rabbitmq.client.Channel;

public class CodecRegistry {
    private static Map<String, AmqpSender> senders = new HashMap<String, AmqpSender>();
    private static Map<String, Class<? extends AMQPMessageConsumer>> consumers = new HashMap<String, Class<? extends AMQPMessageConsumer>>();
    
    public static AmqpSender getSender(String contentType) {
        return senders.get(contentType);
    }
    
    @SuppressWarnings("unchecked")
    public static AMQPMessageConsumer getConsumer(String contentType, Channel channel, Handler<AMQPMessageConsumer.AmqpResponse> handler) throws NoSuchMethodException {
        Class<AMQPMessageConsumer> klass = (Class<AMQPMessageConsumer>)consumers.get(contentType);
        Constructor<AMQPMessageConsumer> c;
        try {
            c = klass.getDeclaredConstructor(Channel.class, (Class<Handler<AMQPMessageConsumer.AmqpResponse>>)(Class<?>)Handler.class);
        } catch (NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
            throw new NoSuchMethodException();
        }
        try {
            return (AMQPMessageConsumer) c.newInstance(channel, handler);
        } catch (InstantiationException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            e.printStackTrace();
            throw new NoSuchMethodException();
        }
    }
    
    public static void  register(String contentType, AmqpSender sender) {
        senders.put(contentType,  sender);
    }
    public static void  register(String contentType, Class<? extends AMQPMessageConsumer> consumer) {
        consumers.put(contentType,  consumer);
    }

}
