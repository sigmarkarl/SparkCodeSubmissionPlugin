package com.netapp.spark;

import org.apache.spark.sql.SparkSession;

public class SparkCodeSubmissionServer implements AutoCloseable {
    SparkSession spark;
    int port = -1;

    public SparkCodeSubmissionServer() {
        spark = SparkSession.builder().getOrCreate();
    }

    public SparkCodeSubmissionServer(int port) {
        this();
        this.port = port;
    }

    public SparkCodeSubmissionServer(String master) {
        if (master!=null) {
            if (!master.equalsIgnoreCase("none")) {
                spark = SparkSession.builder().master(master).appName("SparkCodeSubmissionServer").getOrCreate();
            }
        } else {
            spark = SparkSession.builder().getOrCreate();
        }
    }

    public SparkCodeSubmissionServer(int port, String master) {
        this(master);
        this.port = port;
    }

    public void start() {
        var server = new SparkCodeSubmissionDriverPlugin(port);
        server.startCodeSubmissionServer(spark);
    }

    public static void main(String[] args) {
        switch (args.length) {
            case 0 -> new SparkCodeSubmissionServer().start();
            case 1 -> {
                if (args[0].matches("\\d+")) {
                    new SparkCodeSubmissionServer(Integer.parseInt(args[0])).start();
                } else {
                    new SparkCodeSubmissionServer(args[0]).start();
                }
            }
            case 2 -> new SparkCodeSubmissionServer(Integer.parseInt(args[0]), args[1]).start();
            default -> new SparkCodeSubmissionServer().start();
        }
    }


    @Override
    public void close() {
        spark.close();
    }
}
