package com.netapp.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.PluginContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class SparkConnectWebsocketTranscodeDriverPlugin implements org.apache.spark.api.plugin.DriverPlugin {
    static String DEFAULT_SUBMISSION_WEBSOCKET_URL = "wss://api.spotinst.io/ocean/spark/cluster/%s/app/%s/%s?accountId=%s";
    static Logger logger = LoggerFactory.getLogger(SparkConnectWebsocketTranscodeDriverPlugin.class);
    ExecutorService transcodeThreads;
    List<Integer> ports = Collections.emptyList();
    String urlstr;
    Map<String,String> headers;

    public SparkConnectWebsocketTranscodeDriverPlugin(List<Integer> ports, String url, String header) {
        this();
        this.ports = ports;
        this.urlstr = url;
        this.headers = initHeaders(header);
    }

    public SparkConnectWebsocketTranscodeDriverPlugin() {
        transcodeThreads = Executors.newFixedThreadPool(10);
    }

    WebSocket getWebSocket(WritableByteChannel channel) {
        var wsListener = new SparkCodeSubmissionWebSocketListener();
        wsListener.setChannel(channel);

        var client = HttpClient.newHttpClient();
        var webSocketBuilder = client.newWebSocketBuilder();
        for (var h : headers.entrySet()) {
            webSocketBuilder = webSocketBuilder.header(h.getKey(), h.getValue());
        }
        return webSocketBuilder.buildAsync(java.net.URI.create(urlstr), wsListener).join();
    }

    void servePort(int port) {
        servePort(port, 2);
    }

    void servePort(int port, int version) {
        System.err.println("Starting server on port " + port);
        try (var serverSocket = new ServerSocket(port)) {
            var running = true;
            while (running) {
                System.err.println("Waiting for connection on port " + port);
                var socket = serverSocket.accept();
                System.err.println("Got connection on port " + port);
                transcodeThreads.submit(() -> {
                    try (socket) {
                        var bb = ByteBuffer.allocate(1024 * 1024);
                        int offset = 0;
                        if (version==2) {
                            bb.putInt(port);
                            offset = 4;
                        }
                        var output = socket.getOutputStream();
                        var input = socket.getInputStream();
                        var channel = Channels.newChannel(output);

                        var webSocket = getWebSocket(channel);
                        var timerTask = new TimerTask() {
                            @Override
                            public void run() {
                                logger.info("sending ping");
                                webSocket.sendPing(ByteBuffer.wrap("ping".getBytes()));
                            }
                        };
                        var timer = new java.util.Timer();
                        timer.schedule(timerTask, 5000, 5000);
                        var bba = bb.array();
                        while (true) {
                            var available = Math.max(input.available(), 1);
                            var read = input.read(bba, offset, Math.min(available, bba.length - offset));
                            if (read == -1) {
                                break;
                            } else {
                                        /*if (webSocket.isInputClosed() || webSocket.isOutputClosed()) {
                                            webSocket.sendClose(200, "Spark Connect closed");
                                            webSocket = getWebSocket(channel);
                                        }*/
                                webSocket.sendBinary(ByteBuffer.wrap(bba, 0, read+offset), true);
                            }
                            offset = 0;
                        }
                        webSocket.sendText("loft", true);
                        webSocket.sendClose(200, "Spark Connect closed");
                        timer.cancel();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            System.err.println("Server on port " + port + " closed");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void startTranscodeServers(int version) {
        logger.info("Starting code submission server");
        boolean fetchPorts = false;
        for (int port : ports) {
            transcodeThreads.submit(() -> servePort(port, version));
            fetchPorts = fetchPorts | port == 10000;
        }
        if (fetchPorts) {
            try (var connection = DriverManager.getConnection("jdbc:hive2://localhost:10000"); var statement = connection.createStatement();) {
                var resultSet = statement.executeQuery("SELECT * FROM global_temp.spark_connect_info");
                while (resultSet.next()) {
                    var type = resultSet.getString(1);
                    var langport = resultSet.getInt(2)+10;
                    var secret = resultSet.getString(3);
                    if (type.equals("py4j")) {
                        System.err.println("export PYSPARK_PIN_THREAD=true");
                        System.err.println("export PYSPARK_GATEWAY_PORT=" + langport);
                        System.err.println("export PYSPARK_GATEWAY_SECRET=" + secret);
                        transcodeThreads.submit(() -> servePort(langport));
                    } else if(type.equals("rbackend")) {
                        System.err.println("export EXISTING_SPARKR_BACKEND_PORT=" + langport);
                        System.err.println("export SPARKR_BACKEND_AUTH_SECRET=" + secret);
                        transcodeThreads.submit(() -> servePort(langport));
                    }
                }
            } catch (Exception e) {
                logger.error("Error getting spark connect info", e);
            }
        }
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
        if (ports.size() == 0) {
            ports = Arrays.stream(sc.getConf().get("spark.code.submission.ports", "15002").split(",")).mapToInt(Integer::parseInt).boxed().collect(Collectors.toList());
        }
        if (urlstr == null) {
            var fallbackUrl = "ws://localhost:9000";
            var accountId = sc.getConf().get("spark.code.submission.accountId");
            if (accountId != null && !accountId.isEmpty()) {
                var clusterId = sc.getConf().get("spark.code.submission.clusterId");
                var appId = sc.getConf().get("spark.code.submission.appId");
                var entryPoint = sc.getConf().get("spark.code.submission.entryPoint", "connect");
                fallbackUrl = String.format(DEFAULT_SUBMISSION_WEBSOCKET_URL, clusterId, appId, entryPoint, accountId);
            }
            urlstr = sc.getConf().get("spark.code.submission.url", fallbackUrl);
        }
        if (headers == null) {
            headers = initHeaders(sc.getConf().get("spark.code.submission.headers", ""));
        }
        var token = sc.getConf().get("spark.code.submission.token");
        if (token != null && !token.isEmpty()) {
            headers.put("Authorization", "Bearer "+token);
        }
        var version = Integer.parseInt(sc.getConf().get("spark.code.submission.version", "2"));
        startTranscodeServers(version);
        return Collections.emptyMap();
    }

    @Override
    public void shutdown() {
        transcodeThreads.shutdown();
    }

    public static void main(String[] args) {
        var ports = Arrays.stream(args[0].split(";")).mapToInt(Integer::parseInt).boxed().collect(Collectors.toList());
        var url = args[1];
        var auth = args.length > 2 ? args[2] : "";
        var plugin = new SparkConnectWebsocketTranscodeDriverPlugin(ports, url, auth);
        plugin.startTranscodeServers(2);
    }
}
