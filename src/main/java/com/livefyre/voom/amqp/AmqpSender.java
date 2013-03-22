package com.livefyre.voom.amqp;

import java.io.IOException;

import javax.mail.MessagingException;

import com.livefyre.voom.VoomMessage;
import com.rabbitmq.client.Channel;

public interface AmqpSender {
    void send(Channel channel, VoomMessage message) throws IOException, MessagingException;
}
