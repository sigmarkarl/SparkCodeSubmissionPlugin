package com.netapp.spark;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.util.Config;
import org.apache.spark.api.plugin.PluginContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class SparkCodeSubmissionExecutorPlugin implements org.apache.spark.api.plugin.ExecutorPlugin {
    static Logger logger = LoggerFactory.getLogger(SparkCodeSubmissionExecutorPlugin.class);

    CoreV1Api coreV1Api;
    long    startTimeMs;
    String  nodeType;
    String  hostname;
    String  appId;
    @Override
    public void init(PluginContext ctx, Map<String, String> extraConf) {
        startTimeMs = System.currentTimeMillis();
        try {
            var client = Config.defaultClient();
            Configuration.setDefaultApiClient(client);
            coreV1Api = new CoreV1Api();

            hostname = InetAddress.getLocalHost().getHostName();
            System.err.println("Hostname1: " + hostname);
            hostname = ctx.hostname();
            System.err.println("Hostname2: " + hostname);
            appId = ctx.conf().getAppId();
            System.err.println("AppId: " + appId);
            Arrays.stream(ctx.conf().getAll()).forEach((t) -> System.err.println(t._1 + ": " + t._2));
            extraConf.forEach((key, value) -> System.err.println(key + ": " + value));
            ctx.resources().forEach((key, value) -> System.err.println(key + ": " + value));

            var pod = coreV1Api.readNamespacedPod(hostname, "spark-apps",null);
            var nodeName = Objects.requireNonNull(pod.getSpec()).getNodeName();
            var node = coreV1Api.readNode(nodeName, null);
            nodeType = node.getMetadata().getLabels().get("beta.kubernetes.io/instance-type");
        } catch (IOException | ApiException e) {
            logger.error("Unable to initialize Kubernetes client", e);
        }
    }

    @Override
    public void shutdown() {
        var client = HttpClient.newHttpClient();
        var url = "http://"+appId+"-driver";

        // Create the request body
        var endTimeMs = System.currentTimeMillis();
        var code = "{\"hostname\": \""+hostname+"\", \"nodeType\": \""+nodeType+"\", \"startTimeMs\": "+startTimeMs+", \"endTimeMs\": "+endTimeMs+"}";
        var requestBody = "{\"type\":\"cost\", \"code\": \""+code+"\"}";
        System.err.println("about to send: " + requestBody + " to " + url);

        // Build the request
        var request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        // Send the request
        try {
            var response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
            System.err.println("Response: " + response.body());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
