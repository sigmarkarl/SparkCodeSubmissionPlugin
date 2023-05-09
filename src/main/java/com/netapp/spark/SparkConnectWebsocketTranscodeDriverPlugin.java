package com.netapp.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SparkConnectWebsocketTranscodeDriverPlugin implements org.apache.spark.api.plugin.DriverPlugin {
    static Logger logger = LoggerFactory.getLogger(SparkConnectWebsocketTranscodeDriverPlugin.class);
    ExecutorService transcodeThread;
    int port = -1;
    String urlstr;
    Map<String,String> headers;
    WebSocket   webSocket;

    public SparkConnectWebsocketTranscodeDriverPlugin(int port, String url, String header) {
        this();
        this.port = port;
        this.urlstr = url;
        this.headers = initHeaders(header);
    }

    public SparkConnectWebsocketTranscodeDriverPlugin() {
        transcodeThread = Executors.newSingleThreadExecutor();
    }

    void startTranscodeServer() {
        logger.info("Starting code submission server");
        transcodeThread.submit(() -> {
            try {
                var client = HttpClient.newHttpClient();
                var bb = new byte[1024*1024];
                var wsListener = new SparkCodeSubmissionWebSocketListener();
                var serverSocket = new ServerSocket(port);
                var running = true;
                while (running) {
                    var socket = serverSocket.accept();
                    var output = socket.getOutputStream();
                    var input = socket.getInputStream();
                    var channel = Channels.newChannel(output);
                    wsListener.setChannel(channel);

                    var webSocketBuilder = client.newWebSocketBuilder();
                    for (var h : headers.entrySet()) {
                        webSocketBuilder = webSocketBuilder.header(h.getKey(), h.getValue());
                    }
                    var webSocket = webSocketBuilder.buildAsync(java.net.URI.create(urlstr), wsListener).join();

                    while (true) {
                        var available = Math.max(input.available(), 1);
                        System.err.println("local available " + available);
                        var read = input.read(bb, 0, Math.min(available, bb.length));
                        System.err.println("local read " + read);
                        if (read == -1) {
                            break;
                        } else {
                            webSocket.sendBinary(ByteBuffer.wrap(bb, 0, read), true);
                            System.err.println("local after send");
                        }
                    }
                    webSocket.sendText("loft", true);
                    webSocket.sendClose(200, "Spark Connect closed");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    Map<String,String> initHeaders(String header) {
        var headers = new HashMap<String,String>();
        var hsplit = header.split(",");
        for (var h : hsplit) {
            var i = h.indexOf('=');
            if (i != -1) headers.put(h.substring(0,i), h.substring(i+1));
        }
        return headers;
    }

    @Override
    public Map<String,String> init(SparkContext sc, PluginContext myContext) {
        var session = new SparkSession(sc);
        if (port == -1) {
            port = Integer.parseInt(session.sparkContext().getConf().get("spark.code.submission.port", "9001"));
        }
        if (urlstr == null) {
            urlstr = session.sparkContext().getConf().get("spark.code.submission.url", "ws://localhost:9000");
        }
        if (headers == null) {
            headers = initHeaders(session.sparkContext().getConf().get("spark.code.submission.headers", ""));
        }
        startTranscodeServer();
        return Collections.emptyMap();
    }

    @Override
    public void shutdown() {
        transcodeThread.shutdown();
    }

    public static void main(String[] args) {
        var plugin = new SparkConnectWebsocketTranscodeDriverPlugin(Integer.parseInt(args[0]), args[1], args.length > 2 ? args[2] : "");
        plugin.startTranscodeServer();
    }
}
