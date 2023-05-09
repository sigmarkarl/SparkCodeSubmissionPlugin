package com.netapp.spark;

import java.io.IOException;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletionStage;

public class SparkCodeSubmissionWebSocketListener implements WebSocket.Listener {
    WritableByteChannel channel;

    public SparkCodeSubmissionWebSocketListener() {
        super();
    }

    public void setChannel(WritableByteChannel channel) {
        this.channel = channel;
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        System.err.println("opening");
        WebSocket.Listener.super.onOpen(webSocket);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        System.err.println("error");
        error.printStackTrace();
        WebSocket.Listener.super.onError(webSocket, error);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        System.err.println("closing");
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
            throw new RuntimeException(e);
        }
        return WebSocket.Listener.super.onBinary(webSocket, data, last);
    }
}
