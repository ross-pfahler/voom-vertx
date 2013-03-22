package com.livefyre.voom.amqp.mime;

import java.io.IOException;


import javax.mail.MessagingException;

import org.vertx.java.core.Handler;

import com.livefyre.voom.ProtobufLoader;
import com.livefyre.voom.ProtobufLoader.ProtobufLoadError;
import com.livefyre.voom.VoomMessage;
import com.livefyre.voom.amqp.AmqpConsumer;
import com.livefyre.voom.codec.mime.MimeDecoder;

import com.rabbitmq.client.Channel;

public class MimeMessageConsumer extends AmqpConsumer {
    
    private MimeDecoder decoder = new MimeDecoder(new ProtobufLoader("com.livefyre."));
    
    public MimeMessageConsumer(Channel channel, Handler<AmqpConsumer.AmqpResponse> handler) {
        super(channel, handler);
    }

    public VoomMessage decode(final byte[] body) throws IOException {
        // TODO: We're only handling multipart/mixed because that's what
        // we're getting from the other side, handle other content types?
        try {
            return decoder.decodeMultipart(body);
        } catch (MessagingException | ProtobufLoadError e) {
            throw new IOException();
        }
    }
}