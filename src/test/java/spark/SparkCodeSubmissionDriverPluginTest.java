package spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netapp.spark.CodeSubmission;
import com.netapp.spark.CodeSubmissionType;
import com.netapp.spark.SparkCodeSubmissionDriverPlugin;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class SparkCodeSubmissionDriverPluginTest {
    static SparkSession spark;
    static ObjectMapper mapper = new ObjectMapper();
    static SparkCodeSubmissionDriverPlugin sparkCodeSubmissionDriverPlugin;

    @BeforeAll
    public static void setup() {
        spark = SparkSession.builder().master("local").getOrCreate();
        spark.sql("select random()").count();
        sparkCodeSubmissionDriverPlugin = new SparkCodeSubmissionDriverPlugin(58979);
        sparkCodeSubmissionDriverPlugin.init(spark.sparkContext(), null);
    }

    private void checkResultsFile() throws IOException {
        var resultsFile = Path.of("test.csv");
        Assertions.assertTrue(Files.exists(resultsFile));
        try (var stream = Files.walk(resultsFile)) {
            var csvPath = stream.filter(Files::isRegularFile).filter(path -> path.getFileName().toString().endsWith("csv")).findFirst().get();
            var result = Files.readString(csvPath).trim();
            try {
                Double.parseDouble(result);
            } catch (NumberFormatException e) {
                Assertions.fail("Result is not a number: "+result);
            }
        }
        try (var resultsDir = Files.walk(resultsFile)) {
            resultsDir.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    // Don't care
                }
            });
        }
    }

    void testSparkSubmissionDriverPlugin(CodeSubmission codeSubmission) throws IOException, ClassNotFoundException, NoSuchMethodException, InterruptedException, URISyntaxException, ExecutionException {
        sparkCodeSubmissionDriverPlugin.submitCode(spark, codeSubmission, mapper);
        sparkCodeSubmissionDriverPlugin.waitForVirtualThreads();
        checkResultsFile();
    }

    @Test
    public void testSparkSQLSubmissionDriverPlugin() throws IOException, ClassNotFoundException, NoSuchMethodException, InterruptedException, URISyntaxException, ExecutionException {
        var codeSubmission = new CodeSubmission(CodeSubmissionType.SQL, "select random()", "", List.of(), Map.of(), "", "csv", "test.csv");
        testSparkSubmissionDriverPlugin(codeSubmission);
    }

    static String JAVA_CODE = """
        import org.apache.spark.sql.SparkSession;
        import org.apache.spark.sql.types.DataTypes;
        import org.apache.spark.sql.api.java.UDF1;
        import java.util.Random;
        public class Test {
            public static void main(String[] args) {
                SparkSession spark = SparkSession.builder().appName("test").getOrCreate();
                spark.udf().register("mysqr", new UDF1<Double, Double>() {
                    @Override
                    public Double call(Double x) throws Exception {
                        return x*x;
                    }
                }, DataTypes.DoubleType);
                spark.sql("select mysqr(random())").write().format("csv").mode("overwrite").save(args[0]);
            }
        }
        """;

    @Test
    public void testSparkJavaSubmissionDriverPlugin() throws IOException, ClassNotFoundException, NoSuchMethodException, InterruptedException, URISyntaxException, ExecutionException {
        var codeSubmission = new CodeSubmission(CodeSubmissionType.JAVA, JAVA_CODE, "Test", List.of("test.csv"), Map.of(),"", "", "");
        testSparkSubmissionDriverPlugin(codeSubmission);
    }

    static String PYTHON_CODE = """
        import sys
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        spark.sql("select random()").write.format("csv").mode("overwrite").save(sys.argv[1])
        """;

    @Test
    public void testSparkPythonSubmissionDriverPlugin() throws IOException, ClassNotFoundException, NoSuchMethodException, InterruptedException, URISyntaxException, ExecutionException {
        var codeSubmission = new CodeSubmission(CodeSubmissionType.PYTHON, PYTHON_CODE, "", List.of("test.csv"), Map.of(),"", "", "");
        testSparkSubmissionDriverPlugin(codeSubmission);
    }

    void testSparkSubmissionToServer(CodeSubmission codeSubmission) throws IOException, InterruptedException {
        var codeSubmissionJSON = mapper.writeValueAsString(codeSubmission);
        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder()
                .uri(java.net.URI.create("http://localhost:"+sparkCodeSubmissionDriverPlugin.getPort()))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(codeSubmissionJSON))
                .build();
        var response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals(codeSubmission.type()+" code submitted", response.body());
        sparkCodeSubmissionDriverPlugin.waitForVirtualThreads();
        checkResultsFile();
    }

    @Test
    public void testObjectMapper() throws IOException {
        var codeSubmission = new CodeSubmission(CodeSubmissionType.SQL, "select random()", "", List.of(), Map.of(), "", "csv", "test.csv");
        var codeSubmissionJSON = mapper.writeValueAsString(codeSubmission);
        var codeSubmission2 = mapper.readValue(codeSubmissionJSON, CodeSubmission.class);
        Assertions.assertEquals(codeSubmission, codeSubmission2);
    }

    @Test
    public void testSparkSQLSubmissionToServer() throws IOException, InterruptedException {
        var codeSubmission = new CodeSubmission(CodeSubmissionType.SQL, "select random()", "", List.of(), Map.of(), "", "csv", "test.csv");
        testSparkSubmissionToServer(codeSubmission);
    }

    @Test
    public void testSparkPythonSubmissionToServer() throws IOException, InterruptedException {
        var pythonBase64 = Base64.getEncoder().encodeToString(PYTHON_CODE.getBytes());
        var codeSubmission = new CodeSubmission(CodeSubmissionType.PYTHON_BASE64, pythonBase64, "", List.of("test.csv"), Map.of(),"", "", "");
        testSparkSubmissionToServer(codeSubmission);
    }

    @Test
    @Disabled
    public void testSparkJavaSubmissionToServer() {
        sparkCodeSubmissionDriverPlugin.init(spark.sparkContext(), null);
        var codeSubmission = new CodeSubmission(CodeSubmissionType.JAVA, "public class TestClass { public static String testMethod() { return \"Hello World!\"; } }", "TestClass", List.of(), Map.of(),"", "", "");
        sparkCodeSubmissionDriverPlugin.shutdown();
    }

    @AfterAll
    public static void teardown() {
        sparkCodeSubmissionDriverPlugin.shutdown();
        spark.stop();
    }
}
