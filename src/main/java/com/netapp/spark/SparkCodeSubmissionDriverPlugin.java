package com.netapp.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.undertow.Undertow;

import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.BlockingHandler;
import io.undertow.util.Headers;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.spi.WebSocketHttpExchange;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.api.python.Py4JServer;
import org.apache.spark.api.r.RAuthHelper;
import org.apache.spark.api.r.RBackend;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.connect.service.SparkConnectService;
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2;
import org.apache.spark.sql.hive.thriftserver.ui.HiveThriftServer2EventManager;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static io.undertow.Handlers.websocket;

public class SparkCodeSubmissionDriverPlugin implements org.apache.spark.api.plugin.DriverPlugin {
    static Logger logger = LoggerFactory.getLogger(SparkCodeSubmissionDriverPlugin.class);
    Undertow codeSubmissionServer;
    int port;
    ExecutorService virtualThreads;
    ExecutorService transcodeThread;
    Py4JServer py4jServer;
    int pyport;
    String secret;
    RBackend rBackend;
    int rbackendPort;
    String rbackendSecret;
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

    public SparkCodeSubmissionDriverPlugin() {
        this(-1);
    }

    public SparkCodeSubmissionDriverPlugin(int port) {
        this.port = port;
        virtualThreads = Executors.newVirtualThreadPerTaskExecutor();
        transcodeThread = Executors.newSingleThreadExecutor();
    }

    public int getPort() {
        return port;
    }

    private Row initPy4JServer(SparkContext sc) throws IOException, NoSuchFieldException, IllegalAccessException {
        alterPysparkInitializeContext();

        var path = System.getenv("_PYSPARK_DRIVER_CONN_INFO_PATH");
        if (path != null && !path.isEmpty()) {
            var infopath = Path.of(path);
            if (Files.exists(infopath)) {
                try (var walkStream = Files.walk(infopath)) {
                    walkStream
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().endsWith(".info"))
                            .findFirst()
                            .ifPresent(p -> {
                                try {
                                    var connInfo = new DataInputStream(Files.newInputStream(p));
                                    pyport = connInfo.readInt();
                                    connInfo.readInt();
                                    secret = connInfo.readUTF();
                                } catch (IOException e) {
                                    logger.error("Failed to delete file: " + p, e);
                                }
                            });
                }
            }
        }

        if (secret==null || secret.isEmpty()) {
            py4jServer = new Py4JServer(sc.conf());
            pyport = py4jServer.getListeningPort();
            secret = py4jServer.secret();
            py4jServer.start();

            Map<String, String> sysenv = System.getenv();
            Field field = sysenv.getClass().getDeclaredField("m");
            field.setAccessible(true);
            var env = ((Map<String, String>) field.get(sysenv));
            env.put("PYSPARK_GATEWAY_PORT", Integer.toString(pyport));
            env.put("PYSPARK_GATEWAY_SECRET", secret);
            env.put("PYSPARK_PIN_THREAD", "true");
        }
        return RowFactory.create("py4j", pyport, secret);
    }

    private Row initRBackend() {
        rBackend = new RBackend();
        Tuple2<Object, RAuthHelper> tuple = rBackend.init();
        rbackendPort = (Integer) tuple._1;
        rbackendSecret = tuple._2.secret();

        new Thread(() -> rBackend.run()).start();
        return RowFactory.create("rbackend", rbackendPort, rbackendSecret);
    }

    private Process runProcess(List<String> arguments, Map<String,String> environment, String processName) throws IOException {
        return runProcess(arguments, environment, processName, true);
    }

    private Process runProcess(List<String> arguments, Map<String,String> environment, String processName, boolean inheritOutput) throws IOException {
        var args = new ArrayList<>(Collections.singleton(processName));
        args.addAll(arguments);
        var pb  = new ProcessBuilder(args);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        if (inheritOutput) pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        //pb.redirectError(ProcessBuilder.Redirect.to(Path.of("python-err.log").toFile()));
        //pb.redirectOutput(ProcessBuilder.Redirect.to(Path.of("python-out.log").toFile()));

        var env = pb.environment();
        if (environment != null) env.putAll(environment);

        if (secret != null) {
            env.put("PYSPARK_GATEWAY_PORT", Integer.toString(pyport));
            env.put("PYSPARK_GATEWAY_SECRET", secret);
            env.put("PYSPARK_PIN_THREAD", "true");
        }

        if (rbackendSecret != null) {
            env.put("EXISTING_SPARKR_BACKEND_PORT", Integer.toString(rbackendPort));
            env.put("SPARKR_BACKEND_AUTH_SECRET", rbackendSecret);
        }

        var process = pb.start();
        virtualThreads.submit(() -> {
            try {
                int exitCode = process.waitFor();
                if (exitCode != 0) {
                    logger.error(processName+" process exited with code " + exitCode);
                }
            } catch (InterruptedException e) {
                logger.error(processName+" Execution failed", e);
            }
        });
        return process;
    }

    private Process runPython(String pythonCode, List<String> pythonArgs, Map<String,String> pythonEnv) throws IOException {
        return runPython(pythonCode, pythonArgs, pythonEnv, true);
    }

    private Process runPython(String pythonCode, List<String> pythonArgs, Map<String,String> pythonEnv, boolean inheritOutput) throws IOException {
        var pysparkPython = System.getenv("PYSPARK_PYTHON");
        var cmd = pysparkPython != null ? pysparkPython : "python3";
        var file = Files.createTempFile("python", ".py");
        Files.writeString(file, pythonCode);
        var args = new ArrayList<>(Collections.singleton(file.toString()));
        if (pythonArgs != null) args.addAll(pythonArgs);
        return runProcess(args, pythonEnv, cmd, inheritOutput);
    }

    public String submitCode(SparkSession sqlContext, CodeSubmission codeSubmission, ObjectMapper mapper) throws IOException, ClassNotFoundException, NoSuchMethodException, URISyntaxException, ExecutionException, InterruptedException {
        var defaultResponse = codeSubmission.type() + " code submitted";
        switch (codeSubmission.type()) {
            case SQL -> {
                var sqlCode = codeSubmission.code();
                var resultsPath = codeSubmission.resultsPath();
                if (resultsPath == null || resultsPath.isEmpty()) {
                    if (sqlContext==null) {
                        var code = """
                            import sys
                            from pyspark.sql import SparkSession
                            spark = SparkSession.builder.getOrCreate()
                            json = spark.sql("%s").toPandas().to_json()
                            print(json)
                            """;
                        var pythonCode = String.format(code, sqlCode);
                        System.err.println("about to run " + pythonCode);
                        var process = runPython(pythonCode, codeSubmission.arguments(), codeSubmission.environment(), false);
                        defaultResponse = new String(process.getInputStream().readAllBytes());
                    } else {
                        defaultResponse = virtualThreads.submit(() -> {
                            var df = sqlContext.sql(sqlCode);
                            switch (codeSubmission.resultFormat()) {
                                case "json" -> {
                                    var json = df.toJSON().collectAsList();
                                    return mapper.writeValueAsString(json);
                                }
                                case "csv" -> {
                                    return String.join(",", df.collectAsList().toArray(String[]::new));
                                }
                                default -> {
                                    var rows = df.collectAsList();
                                    return mapper.writeValueAsString(rows);
                                }
                            }
                        }).get();
                    }
                } else {
                    if (sqlContext==null) {
                        var code = """
                            import sys
                            from pyspark.sql import SparkSession
                            spark = SparkSession.builder.getOrCreate()
                            spark.sql("%s").write.format("%s").mode("overwrite").save("%s")
                            """;
                        runPython(
                                String.format(code, codeSubmission.code(), codeSubmission.resultFormat(), codeSubmission.resultFormat()),
                                codeSubmission.arguments(),
                                codeSubmission.environment());
                    } else {
                        virtualThreads.submit(() -> sqlContext.sql(sqlCode)
                                .write()
                                .format(codeSubmission.resultFormat())
                                .mode(SaveMode.Overwrite)
                                .save(codeSubmission.resultsPath()));
                    }
                }
            }
            case PYTHON -> runPython(codeSubmission.code(), codeSubmission.arguments(), codeSubmission.environment());
            case PYTHON_BASE64 -> {
                var pythonCodeBase64 = codeSubmission.code();
                var pythonCode = new String(Base64.getDecoder().decode(pythonCodeBase64));
                runPython(pythonCode, codeSubmission.arguments(), codeSubmission.environment());
            }
            case R -> virtualThreads.submit(() -> {
                try {
                    var processName = "Rscript";
                    var RProcess = runProcess(codeSubmission.arguments(), codeSubmission.environment(), processName);
                    RProcess.getOutputStream().write(codeSubmission.code().getBytes());
                } catch (IOException e) {
                    logger.error("R Execution failed: ", e);
                }
            });
            case JAVA -> {
                var classLoader = this.getClass().getClassLoader();
                Class<?> submissionClass = null;
                try {
                    submissionClass = classLoader.loadClass(codeSubmission.className());
                } catch (ClassNotFoundException e) {
                    var javaCode = codeSubmission.code();
                    var url = classLoader.getResource("test.txt");
                    assert url != null;
                    var classPath = Path.of(codeSubmission.className() + ".java");
                    var path = Path.of(url.toURI()).getParent().resolve(classPath);
                    Files.writeString(path, javaCode);
                    int exitcode = compiler.run(System.in, System.out, System.err, path.toString());
                    if (exitcode != 0) {
                        defaultResponse = "Java Compilation failed: " + exitcode;
                        logger.error(defaultResponse);
                    } else {
                        submissionClass = classLoader.loadClass(codeSubmission.className());
                    }
                }

                if (submissionClass!=null) {
                    var mainMethod = submissionClass.getMethod("main", String[].class);
                    virtualThreads.submit(() -> {
                        try {
                            mainMethod.invoke(null, (Object) codeSubmission.arguments().toArray(String[]::new));
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            logger.error("Java Execution failed: ", e);
                        }
                    });
                }
            }
            case COMMAND -> {
                var processName = codeSubmission.code();
                /*args.add("console");
                args.add("--kernel");
                args.add("sparkmagic_kernels.pysparkkernel");
                args.add("--existing");
                args.add("kernel-" + pyport + ".json");*/
                runProcess(codeSubmission.arguments(), codeSubmission.environment(), processName);
            }
            default -> logger.error("Unknown code type: " + codeSubmission.type());
        }
        return defaultResponse;
    }

    private void fixContext(Path pysparkPath) {
        if (Files.exists(pysparkPath)) {
            try {
                var oldStatement = "return self._jvm.JavaSparkContext(jconf)";
                var pyspark = Files.readString(pysparkPath);
                if (pyspark.contains(oldStatement)) {
                    pyspark = pyspark.replace(oldStatement, "return self._jvm.JavaSparkContext.fromSparkContext(self._jvm.org.apache.spark.SparkContext.getOrCreate(jconf))");
                    Files.writeString(pysparkPath, pyspark);
                    logger.info("Pyspark initialize context altered");
                }
            } catch (IOException e) {
                logger.error("Failed to alter pyspark initialize context", e);
            }
        }
    }

    private void alterPysparkInitializeContext() {
        var sparkHome = System.getenv("SPARK_HOME");
        if (sparkHome != null) {
            var pysparkPath = Path.of(sparkHome, "python", "pyspark", "context.py");
            fixContext(pysparkPath);
        }
        var pysparkPython = System.getenv("PYSPARK_PYTHON");
        var cmd = pysparkPython != null ? pysparkPython : "python3";
        var processBuilder = new ProcessBuilder(cmd, "-c", "import pyspark, os; print(os.path.dirname(pyspark.__file__))");
        try {
            var process = processBuilder.start();
            var path = new String(process.getInputStream().readAllBytes());
            var pysparkPath = Path.of(path.trim(), "context.py");
            fixContext(pysparkPath);
        } catch (IOException e) {
            logger.info("Failed to alter pyspark initialize context info", e);
        }
    }

    void handlePostRequest(HttpServerExchange exchange, SparkContext sparkContext, ObjectMapper mapper) {
        if (exchange.getRequestMethod().equalToString("POST")) {
            exchange.getRequestReceiver().receiveFullString((ex, codeSubmissionStr) -> {
                try {
                    var session = SparkSession.getActiveSession().isDefined() ? SparkSession.getActiveSession().get() : new SparkSession(sparkContext);
                    if (codeSubmissionStr.contains("appId")) {
                        var jsonMap = mapper.readValue(codeSubmissionStr, Map.class);
                        var configOverrides = jsonMap.get("configOverrides");
                        if (configOverrides != null) {
                            var configOverridesMap = (Map<String, Object>) configOverrides;
                            var arguments = configOverridesMap.get("arguments");
                            if (arguments instanceof List) {
                                var argumentsList = (List<String>) arguments;
                                var args = new ArrayList<String>();
                                args.add("src/main/resources/launch_ipykernel_old.py");
                                args.addAll(argumentsList);
                                runProcess(args, Collections.emptyMap(), "python3");
                            }
                        }
                    } else {
                        var codeSubmission = mapper.readValue(codeSubmissionStr, CodeSubmission.class);
                        var response = submitCode(session, codeSubmission, mapper);
                        exchange.getResponseSender().send(response);
                    }
                } catch (JsonProcessingException e) {
                    logger.error("Failed to parse code submission", e);
                    exchange.getResponseSender().send("Failed to parse code submission");
                } catch (IOException | URISyntaxException | ExecutionException | ClassNotFoundException |
                         InterruptedException | NoSuchMethodException e) {
                    logger.error("Failed to submit code", e);
                    exchange.getResponseSender().send("Failed to submit code");
                }
            });
        } else {
            // Return a 404 response for other request methods
            exchange.setStatusCode(404);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
            exchange.getResponseSender().send("Not found");
        }
    }

    void handleWebsocketRequest(WebSocketHttpExchange exchange, WebSocketChannel channel, String grpcPort, String hivePort) {
        var grpcList = exchange.getRequestParameters().getOrDefault("grpc", List.of(grpcPort));
        var usedGrpc = Integer.parseInt(grpcList.get(0));

        var hiveList = exchange.getRequestParameters().getOrDefault("grpc", List.of(hivePort));
        var usedHive = Integer.parseInt(hiveList.get(0));

        try {
            var sparkReceiveListener = new SparkReceiveListener(virtualThreads, channel, usedHive, usedGrpc);
            channel.getReceiveSetter().set(sparkReceiveListener);
            channel.resumeReceives();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void startCodeSubmissionServer(SparkContext sc) throws IOException {
        var mapper = new ObjectMapper();
        var grpcPort = sc != null ? sc.conf().get("spark.connect.grpc.binding.port", "15002") : "15002";
        var hivePortStr = System.getenv("HIVE_SERVER2_THRIFT_PORT");
        var hivePort = (hivePortStr == null || hivePortStr.isEmpty()) ? "10000" : hivePortStr;

        var webSocketPostHandler = websocket((exchange, channel) -> handleWebsocketRequest(exchange, channel, grpcPort, hivePort), new BlockingHandler(exchange -> handlePostRequest(exchange, sc, mapper)));
        codeSubmissionServer = Undertow.builder()
            .addHttpListener(port, "0.0.0.0")
            .setHandler(webSocketPostHandler)
            .build();

        System.err.println("Using submission port "+ port);
        codeSubmissionServer.start();
    }

    void init(SparkContext sc, SQLContext sqlContext) throws NoSuchFieldException, IllegalAccessException {
        logger.info("Starting code submission server");
        if (port == -1) {
            port = Integer.parseInt(sc.conf().get("spark.code.submission.port", "9001"));
        }
        try {
            var useSparkConnect = sc.conf().get("spark.code.submission.connect", "true");
            var usePySpark = sc.conf().get("spark.code.submission.pyspark", "true");
            var useRBackend = sc.conf().get("spark.code.submission.sparkr", "true");
            var useHive = sc.conf().get("spark.code.submission.hive", "true");
            if (useSparkConnect.equalsIgnoreCase("true")) SparkConnectService.start();
            if (useHive.equalsIgnoreCase("true")) {
                var hiveThriftServer = new HiveThriftServer2(sqlContext);
                var hiveEventManager = new HiveThriftServer2EventManager(sc);
                HiveThriftServer2.eventManager_$eq(hiveEventManager);
                var hiveConf = new HiveConf();
                hiveThriftServer.init(hiveConf);
                hiveThriftServer.start();
            }

            var connectInfo = new ArrayList<Row>();
            if (usePySpark.equalsIgnoreCase("true")) connectInfo.add(initPy4JServer(sc));
            if (useRBackend.equalsIgnoreCase("true")) connectInfo.add(initRBackend());

            var df = sqlContext.createDataset(connectInfo, RowEncoder.apply(StructType.fromDDL("type string, port int, secret string")));
            df.createOrReplaceGlobalTempView("spark_connect_info");
            var connInfoPath = System.getenv("PYSPARK_DRIVER_CONN_INFO_PATH");
            if (connInfoPath != null && !connInfoPath.isEmpty()) df.write().mode(SaveMode.Overwrite).save(connInfoPath);

            startCodeSubmissionServer(sc);
        } catch (RuntimeException e) {
            logger.error("Failed to start code submission server at port: " + port, e);
            throw e;
        } catch (IOException e) {
            logger.error("Unable to alter pyspark context code", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String,String> init(SparkContext sc, PluginContext myContext) {
        try {
            init(sc, new SQLContext(sc));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return Collections.emptyMap();
    }

    @Override
    public void shutdown() {
        if (codeSubmissionServer!=null) codeSubmissionServer.stop();
        if (py4jServer!=null) py4jServer.shutdown();
        if (rBackend!=null) rBackend.close();
        try {
            if (waitForVirtualThreads()) {
                logger.info("Virtual threads finished");
            } else {
                logger.debug("Virtual threads did not finish in time");
            }
        } catch (InterruptedException e) {
            logger.debug("Interrupted while waiting for virtual threads to finish", e);
        }
        virtualThreads.shutdown();
        transcodeThread.shutdown();
    }

    public boolean waitForVirtualThreads() throws InterruptedException {
        return virtualThreads.awaitTermination(10, TimeUnit.SECONDS);
    }

    public static void main(String[] args) {
        var sparkCodeSubmissionPlugin = new SparkCodeSubmissionPlugin();
        var sparkContext = new SparkContext(args[0], args[1]);
        sparkCodeSubmissionPlugin.driverPlugin().init(sparkContext, null);
        var pid = ProcessHandle.current().pid();
        System.err.println("Started code submission server with pid: " + pid);
        var sigchld = new Signal("CHLD");
        Signal.handle(sigchld, SignalHandler.SIG_IGN);
    }
}
