package com.netapp.spark;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;

public class SparkCodeSubmissionClient {
    private final HttpClient client;

    public SparkCodeSubmissionClient() {
        client = HttpClient.newHttpClient();
    }

    public void run(String urlstr, String codeSubmissionJSON) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder()
                .uri(java.net.URI.create(urlstr))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(codeSubmissionJSON))
                .build();
        var response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
        System.out.println(response.statusCode());
        System.out.println(response.body());
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        var client = new SparkCodeSubmissionClient();
        if (args.length == 2) {
            client.run(args[0], args[1]);
        } else {
            var pythonCode = """
                    from pyspark.sql import SparkSession
                    from pyspark.sql.types import IntegerType
                    def delayedSqr(x):
                      import time
                      time.sleep(100)
                      return x*x
                    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
                    spark.udf.register("delayedSqr", delayedSqr, IntegerType())
                    spark.sql("select delayedSqr(random())").write.format("csv").mode("overwrite").save("test.csv")
                    """;
            //var pythonBase64 = java.util.Base64.getEncoder().encodeToString(pythonCode.getBytes());
            /*client.run("http://localhost:9001", String.format("""
                    {
                      "type": "PYTHON_B64",
                      "code": "%s",
                      "className": "",
                      "config": "",
                      "resultFormat": "csv",
                      "resultsPath": "test.csv"
                    }
                    """, pythonBase64));*/
            client.run("http://localhost:9001", """
                    {
                      "type": "SQL",
                      "code": "select random()",
                      "className": "",
                      "config": "",
                      "resultFormat": "json",
                      "resultsPath": ""
                    }
                    """);
        }
    }
}
