package com.netapp.spark;

import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

public class SparkCodeSubmissionPlugin implements SparkPlugin {
    @Override
    public DriverPlugin driverPlugin() {
        return new SparkCodeSubmissionDriverPlugin();
    }

    @Override
    public ExecutorPlugin executorPlugin() {
        return null;
    }
}
