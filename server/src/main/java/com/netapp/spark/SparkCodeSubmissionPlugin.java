package com.netapp.spark;

import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;
import org.apache.spark.sql.SparkSession;

public class SparkCodeSubmissionPlugin implements SparkPlugin {
    @Override
    public DriverPlugin driverPlugin() {
        return new SparkCodeSubmissionDriverPlugin();
    }

    @Override
    public ExecutorPlugin executorPlugin() {
        return null;
    }

    public static void main(String[] args) {
        try (var sparkSession = SparkSession
                .builder()
                .config("spark.plugins","com.netapp.spark.SparkCodeSubmissionPlugin")
                .enableHiveSupport()
                .master("local[*]")
                .appName("TestSpark")
                .getOrCreate()) {
            sparkSession.sql("select random()").show();
        }
    }
}
