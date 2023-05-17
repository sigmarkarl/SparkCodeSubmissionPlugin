package com.netapp.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletionStage;

public class SparkCodeSubmissionWebSocketListener implements WebSocket.Listener {
    static Logger logger = LoggerFactory.getLogger(SparkCodeSubmissionWebSocketListener.class);

    WritableByteChannel channel;

    public SparkCodeSubmissionWebSocketListener() {
        super();
    }

    public void setChannel(WritableByteChannel channel) {
        this.channel = channel;
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        logger.info("open websocket");
        WebSocket.Listener.super.onOpen(webSocket);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        logger.error("websocket error", error);
        WebSocket.Listener.super.onError(webSocket, error);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        System.err.println("websocket close");
        logger.info("closing websocket");
        return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        return WebSocket.Listener.super.onText(webSocket, data, last);
    }

    @Override
    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
        try {
            channel.write(data);
        } catch (IOException e) {
            logger.error("unable to write to grpc socket", e);
        }
        return WebSocket.Listener.super.onBinary(webSocket, data, last);
    }
}
