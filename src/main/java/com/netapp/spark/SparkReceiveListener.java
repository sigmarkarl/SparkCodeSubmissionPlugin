package com.netapp.spark;

import io.undertow.websockets.core.*;

import java.io.IOException;
import java.net.Socket;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class SparkReceiveListener extends AbstractReceiveListener {
    Socket clientSocket;
    WritableByteChannel socketChannel;
    
    SparkReceiveListener(Socket clientSocket) throws IOException {
        this.clientSocket = clientSocket;
        var clientOutput = clientSocket.getOutputStream();
        this.socketChannel = Channels.newChannel(clientOutput);
    }
    
    @Override
    protected void onFullBinaryMessage(WebSocketChannel channel, BufferedBinaryMessage message) {
        try {
            var byteBuffers = message.getData().getResource();
            for (var bb : byteBuffers) {
                socketChannel.write(bb);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void onCloseMessage(CloseMessage cm, WebSocketChannel channel) {
        try {
            System.err.println("close server");
            //clientOutput.close();
            //clientInput.close();
            clientSocket.shutdownInput();
            clientSocket.close();
            super.onCloseMessage(cm, channel);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void onClose(WebSocketChannel webSocketChannel, StreamSourceFrameChannel channel) {
        try {
            System.err.println("erm close");
            super.onClose(webSocketChannel, channel);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void onFullCloseMessage(final WebSocketChannel channel, BufferedBinaryMessage message) {
        try {
            System.err.println("full close server");
            clientSocket.shutdownInput();
            clientSocket.close();
            super.onFullCloseMessage(channel, message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
        try {
            System.err.println("full close server " + message.getData());
            clientSocket.shutdownInput();
            super.onFullTextMessage(channel, message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /*var codeSubmissionStr = message.getData();
        try {
            var codeSubmission = mapper.readValue(codeSubmissionStr, CodeSubmission.class);
            var response = submitCode(session, codeSubmission);
            WebSockets.sendTextBlocking(response, channel);
        } catch (IOException | ClassNotFoundException | NoSuchMethodException | URISyntaxException |
                 ExecutionException | InterruptedException e) {
            logger.error("Failed to parse code submission", e);
            WebSockets.sendText("Failed to parse code submission", channel, null);
        }
    }*/
}
