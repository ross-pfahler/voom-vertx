package com.livefyre.voom.amqp;

import java.io.IOException;

import javax.mail.MessagingException;

import com.livefyre.voom.VoomMessage;
import com.rabbitmq.client.Channel;

public interface AmqpSender<T> {
    void send(Channel channel, VoomMessage<T> message) throws IOException, MessagingException;
}
