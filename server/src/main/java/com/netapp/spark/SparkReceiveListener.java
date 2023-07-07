package com.netapp.spark;

import io.undertow.websockets.core.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class SparkReceiveListener extends AbstractReceiveListener {
    Socket clientSocket;
    WritableByteChannel socketChannel;
    boolean first = true;
    ExecutorService virtualThreads;
    WebSocketChannel channel;
    Map<Integer,Integer>    portMap;

    SparkReceiveListener(ExecutorService virtualThreads, WebSocketChannel channel, Map<Integer,Integer> portMap) throws IOException {
        this.clientSocket = new Socket();
        this.virtualThreads = virtualThreads;
        this.channel = channel;
        this.portMap = portMap;
    }

    void init(int port) throws IOException {
        first = false;
        //int port = bb.get(0) == 1 ? hivePort : grpcPort;
        //System.err.println(bb.get(0) + " " + bb.get(1) + " " + bb.get(2) + " " + bb.get(3) + " " + bb.get(4) + " " + bb.get(5) + " " + bb.get(6));
        int nport = portMap.getOrDefault(port, port);
        if (port == nport+10) {
            clientSocket.connect(new InetSocketAddress("localhost", nport));
        } else {
            clientSocket.connect(new InetSocketAddress("0.0.0.0", nport));
        }
        var clientInput = clientSocket.getInputStream();
        var clientOutput = clientSocket.getOutputStream();
        this.socketChannel = Channels.newChannel(clientOutput);

        virtualThreads.submit(() -> {
            try {
                var cbb = new byte[1024 * 1024];
                while (!clientSocket.isClosed()) {
                    var available = Math.max(clientInput.available(), 1);
                    var read = clientInput.read(cbb, 0, Math.min(available, cbb.length));
                    if (read == -1) {
                        System.err.println("Client closed connection");
                        break;
                    } else {
                        System.err.println("Sending " + read + " bytes");
                        WebSockets.sendBinaryBlocking(ByteBuffer.wrap(cbb, 0, read), channel);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    protected void onFullBinaryMessage(WebSocketChannel channel, BufferedBinaryMessage message) {
        try {
            var byteBuffers = message.getData().getResource();
            for (var bb : byteBuffers) {
                if (first) {
                    init(bb.getInt());
                    socketChannel.write(bb.slice());
                } else {
                    socketChannel.write(bb);
                }
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
