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
            var pythonCode = "from pyspark.sql import SparkSession\n" +
                    "from pyspark.sql.types import IntegerType\n" +
                    "def delayedSqr(x):\n" +
                    "import time\n" +
                    "  time.sleep(100)\n" +
                    "  return x*x\n" +
                    "spark = SparkSession.builder.master(\"local\").appName(\"test\").getOrCreate()\n" +
                    "spark.udf.register(\"delayedSqr\", delayedSqr, IntegerType())\n" +
                    "spark.sql(\"select delayedSqr(random())\").write.format(\"csv\").mode(\"overwrite\").save(\"test.csv\")\n";
            var pythonBase64 = java.util.Base64.getEncoder().encodeToString(pythonCode.getBytes());
            client.run("http://localhost:9001", String.format("{\n" +
                        "'type': 'PYTHON_B64',\n" +
                        "'code': '%s',\n" +
                        "'className': '',\n" +
                        "'config': '',\n" +
                        "'resultFormat': 'csv',\n" +
                        "'resultsPath': 'test.csv'\n" +
                        "}", pythonBase64));
        }
    }
}
