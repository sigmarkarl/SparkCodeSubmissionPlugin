package com.netapp.spark;

import org.apache.spark.sql.SparkSession;

public class SparkCodeSubmissionServer implements AutoCloseable {
    SparkSession spark;
    int port;

    public SparkCodeSubmissionServer(int port, String master) {
        this.port = port;
        if (master!=null) spark = SparkSession.builder().master(master).appName("SparkCodeSubmissionServer").getOrCreate();
    }

    public void start() {
        var server = new SparkCodeSubmissionDriverPlugin(port);
        server.startCodeSubmissionServer(spark);
    }

    public static void main(String[] args) {
        var server = args.length == 2 ? new SparkCodeSubmissionServer(Integer.parseInt(args[0]), args[1]) : new SparkCodeSubmissionServer(Integer.parseInt(args[0]), null);
        server.start();
    }


    @Override
    public void close() {
        spark.close();
    }
}
