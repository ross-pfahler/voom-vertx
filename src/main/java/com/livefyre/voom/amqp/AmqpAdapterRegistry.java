package com.livefyre.voom.amqp;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.vertx.java.core.Handler;

import com.rabbitmq.client.Channel;

public class AmqpAdapterRegistry {
    private static Map<String, AmqpSender> senders = new HashMap<String, AmqpSender>();
    private static Map<String, Class<? extends AmqpConsumer>> consumers = new HashMap<String, Class<? extends AmqpConsumer>>();
    
    public static AmqpSender getSender(String contentType) {
        return senders.get(contentType);
    }
    
    @SuppressWarnings("unchecked")
    public static AmqpConsumer getConsumer(String contentType, Channel channel, Handler<AmqpConsumer.AmqpResponse> handler) throws NoSuchMethodException {
        Class<AmqpConsumer> klass = (Class<AmqpConsumer>)consumers.get(contentType);
        Constructor<AmqpConsumer> c;
        try {
            c = klass.getDeclaredConstructor(Channel.class, (Class<Handler<AmqpConsumer.AmqpResponse>>)(Class<?>)Handler.class);
        } catch (NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
            throw new NoSuchMethodException();
        }
        System.out.print(String.format("Ahandler=%s", handler));
        try {
            return (AmqpConsumer) c.newInstance(channel, handler);
        } catch (InstantiationException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            e.printStackTrace();
            throw new NoSuchMethodException();
        }
    }
    
    public static void  register(String contentType, AmqpSender sender) {
        senders.put(contentType,  sender);
    }
    public static void  register(String contentType, Class<? extends AmqpConsumer> consumer) {
        consumers.put(contentType,  consumer);
    }

}
