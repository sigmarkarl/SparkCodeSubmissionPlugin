package com.netapp.spark;

import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

public class SparkConnectWebsocketTranscodePlugin implements SparkPlugin {
    @Override
    public DriverPlugin driverPlugin() {
        return new SparkConnectWebsocketTranscodeDriverPlugin();
    }

    @Override
    public ExecutorPlugin executorPlugin() {
        return null;
    }
}
