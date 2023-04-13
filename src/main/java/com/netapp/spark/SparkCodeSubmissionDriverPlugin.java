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
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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

    private void initPy4JServer(SparkContext sc) {
        py4jServer = new Py4JServer(sc.conf());
        py4jServer.start();
    }

    private void initRBackend() {
        rBackend = new RBackend();
        Tuple2<Object, RAuthHelper> tuple = rBackend.init();
        rbackendPort = (Integer) tuple._1;
        rbackendSecret = tuple._2.secret();

        new Thread(() -> rBackend.run()).start();
    }

    private Process runPythonProcess(ProcessBuilder pb, Map<String,String> envs) throws IOException {
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        //pb.redirectError(ProcessBuilder.Redirect.to(Path.of("python-err.log").toFile()));
        //pb.redirectOutput(ProcessBuilder.Redirect.to(Path.of("python-out.log").toFile()));

        var env = pb.environment();
        env.putAll(envs);

        var port = py4jServer.getListeningPort();
        var secret = py4jServer.secret();

        env.put("PYSPARK_GATEWAY_PORT", Integer.toString(port));
        env.put("PYSPARK_GATEWAY_SECRET", secret);
        env.put("PYSPARK_PIN_THREAD", "true");

        return pb.start();
    }

    private void runRscript(String query) throws IOException, InterruptedException {
        var pb  = new ProcessBuilder("Rscript");
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        var env = pb.environment();

        env.put("EXISTING_SPARKR_BACKEND_PORT", Integer.toString(rbackendPort));
        env.put("SPARKR_BACKEND_AUTH_SECRET", rbackendSecret);

        var RProcess = pb.start();
        RProcess.getOutputStream().write(query.getBytes());
        int exitCode = RProcess.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("R process exited with code " + exitCode);
        }
    }

    private void runPython(String pythonCode, List<String> pythonArgs, Map<String,String> pythonEnv) throws IOException {
        var pysparkPython = System.getenv("PYSPARK_PYTHON");
        var cmd = pysparkPython != null ? pysparkPython : "python3";
        var args = new ArrayList<>(Collections.singleton(cmd));
        args.addAll(pythonArgs);
        var pb  = new ProcessBuilder(args);
        var pythonProcess = runPythonProcess(pb, pythonEnv);
        if (pythonCode.length() > 0) {
            var processOutput = pythonProcess.getOutputStream();
            processOutput.write(pythonCode.getBytes());
            processOutput.close();
        }
        virtualThreads.submit(() -> {
            try {
                int exitCode = pythonProcess.waitFor();
                if (exitCode != 0) {
                    logger.error("Python process exited with code " + exitCode);
                }
            } catch (InterruptedException e) {
                logger.error("Python Execution failed", e);
            }
        });
    }

    public void submitCode(SQLContext sqlContext, CodeSubmission codeSubmission) throws IOException, ClassNotFoundException, NoSuchMethodException {
        switch (codeSubmission.type()) {
            case SQL -> {
                var sqlCode = codeSubmission.code();
                virtualThreads.submit(() -> sqlContext.sql(sqlCode)
                        .write()
                        .format(codeSubmission.resultFormat())
                        .mode(SaveMode.Overwrite)
                        .save(codeSubmission.resultsPath()));
            }
            case PYTHON -> runPython(codeSubmission.code(), codeSubmission.arguments(), codeSubmission.environment());
            case PYTHON_B64 -> {
                var pythonCodeBase64 = codeSubmission.code();
                var pythonCode = new String(Base64.getDecoder().decode(pythonCodeBase64));
                runPython(pythonCode, codeSubmission.arguments(), codeSubmission.environment());
            }
            case R -> {
                var rCode = codeSubmission.code();
                virtualThreads.submit(() -> {
                    try {
                        runRscript(rCode);
                    } catch (IOException | InterruptedException e) {
                        logger.error("R Execution failed: ", e);
                    }
                });
            }
            case JAVA -> {
                var javaCode = codeSubmission.code();
                var classPath = Path.of(codeSubmission.className() + ".java");
                Files.writeString(classPath, javaCode);
                int exitcode = compiler.run(System.in, System.out, System.err, classPath.getFileName().toString());
                if (exitcode != 0) {
                    logger.error("Java Compilation failed: " + exitcode);
                } else {
                    var submissionClass = this.getClass().getClassLoader().loadClass(codeSubmission.className());
                    var mainMethod = submissionClass.getMethod("main", String[].class);
                    virtualThreads.submit(() -> {
                        try {
                            mainMethod.invoke(null, (Object) new String[]{});
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            logger.error("Java Execution failed: ", e);
                        }
                    });
                }
            }
            default -> logger.error("Unknown code type: " + codeSubmission.type());
        }
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

    private void alterPysparkInitializeContext() throws IOException {
        var sparkHome = System.getenv("SPARK_HOME");
        if (sparkHome != null) {
            var pysparkPath = Path.of(sparkHome, "python", "pyspark", "context.py");
            fixContext(pysparkPath);
        }
        var pysparkPython = System.getenv("PYSPARK_PYTHON");
        var cmd = pysparkPython != null ? pysparkPython : "python3";
        var processBuilder = new ProcessBuilder(cmd, "-c", "import pyspark, os; print(os.path.dirname(pyspark.__file__))");
        var process = processBuilder.start();
        var path = new String(process.getInputStream().readAllBytes());
        var pysparkPath = Path.of(path.trim(), "context.py");
        fixContext(pysparkPath);
    }

    @Override
    public Map<String,String> init(SparkContext sc, PluginContext myContext) {
        logger.info("Starting code submission server");
        try {
            alterPysparkInitializeContext();

            var path = System.getenv("_PYSPARK_DRIVER_CONN_INFO_PATH");
            if (!path.isEmpty()) {
                var infopath = Path.of(path);
                if (Files.exists(infopath)) {
                    var connInfo = Files.readString(infopath);
                    var connInfoParts = connInfo.split(" ");
                    port = Integer.parseInt(connInfoParts[1]);
                } else {
                    System.err.println("Connection info file does not exist: " + path);
                    logger.error("Connection info file does not exist: " + path);
                }
            }

            initPy4JServer(sc);
            initRBackend();

            var mapper = new ObjectMapper();
            var sqlContext = new org.apache.spark.sql.SQLContext(sc);

            if (port == -1) {
                port = Integer.parseInt(sc.getConf().get("spark.code.submission.port", "9001"));
            }

            codeSubmissionServer = Undertow.builder()
                    .addHttpListener(port, "0.0.0.0")
                    .setHandler(new BlockingHandler(exchange -> {
                        var codeSubmissionStr = new String(exchange.getInputStream().readAllBytes());
                        try {
                            var codeSubmission = mapper.readValue(codeSubmissionStr, CodeSubmission.class);
                            submitCode(sqlContext, codeSubmission);
                            exchange.getResponseSender().send(codeSubmission.type() + " code submitted");
                        } catch (JsonProcessingException e) {
                            logger.error("Failed to parse code submission", e);
                            exchange.getResponseSender().send("Failed to parse code submission");
                        }
                    }))
                    .build();

            System.err.println("Using port "+ port);
            codeSubmissionServer.start();
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
