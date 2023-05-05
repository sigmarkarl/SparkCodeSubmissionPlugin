package com.netapp.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.undertow.Undertow;
import io.undertow.server.handlers.BlockingHandler;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.api.python.Py4JServer;
import org.apache.spark.api.r.RAuthHelper;
import org.apache.spark.api.r.RBackend;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connect.service.SparkConnectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

public class SparkCodeSubmissionDriverPlugin implements org.apache.spark.api.plugin.DriverPlugin {
    static Logger logger = LoggerFactory.getLogger(SparkCodeSubmissionDriverPlugin.class);
    Undertow codeSubmissionServer;
    int port;
    ExecutorService virtualThreads;
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
    }

    public int getPort() {
        return port;
    }

    private void initPy4JServer(SparkContext sc) throws IOException {
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
        }
    }

    private void initRBackend() {
        rBackend = new RBackend();
        Tuple2<Object, RAuthHelper> tuple = rBackend.init();
        rbackendPort = (Integer) tuple._1;
        rbackendSecret = tuple._2.secret();

        new Thread(() -> rBackend.run()).start();
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

    public String submitCode(SparkSession sqlContext, CodeSubmission codeSubmission) throws IOException, ClassNotFoundException, NoSuchMethodException, URISyntaxException, ExecutionException, InterruptedException {
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
                                    return new ObjectMapper().writeValueAsString(json);
                                }
                                case "csv" -> {
                                    return String.join(",", df.collectAsList().toArray(String[]::new));
                                }
                                default -> {
                                    var rows = df.collectAsList();
                                    return new ObjectMapper().writeValueAsString(rows);
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

    void startCodeSubmissionServer(SparkSession session) {
        var mapper = new ObjectMapper();
        codeSubmissionServer = Undertow.builder()
                .addHttpListener(port, "0.0.0.0")
                .setHandler(new BlockingHandler(exchange -> {
                    var codeSubmissionStr = new String(exchange.getInputStream().readAllBytes());
                    try {
                        var codeSubmission = mapper.readValue(codeSubmissionStr, CodeSubmission.class);
                        var response = submitCode(session, codeSubmission);
                        exchange.getResponseSender().send(response);
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to parse code submission", e);
                        exchange.getResponseSender().send("Failed to parse code submission");
                    }
                }))
                .build();

        System.err.println("Using port "+ port);
        codeSubmissionServer.start();
    }

    @Override
    public Map<String,String> init(SparkContext sc, PluginContext myContext) {
        logger.info("Starting code submission server");
        if (port == -1) {
            port = Integer.parseInt(sc.getConf().get("spark.code.submission.port", "9001"));
        }
        try {
            SparkConnectService.start();
            initPy4JServer(sc);
            initRBackend();
            var session = new SparkSession(sc);
            startCodeSubmissionServer(session);
        } catch (RuntimeException e) {
            logger.error("Failed to start code submission server at port: " + port, e);
            throw e;
        } catch (IOException e) {
            logger.error("Unable to alter pyspark context code", e);
            throw new RuntimeException(e);
        }

        return Map.of();
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
