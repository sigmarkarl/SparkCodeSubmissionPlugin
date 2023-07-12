package com.netapp.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.undertow.Undertow;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.util.Config;

import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.BlockingHandler;
import io.undertow.util.Headers;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.spi.WebSocketHttpExchange;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
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
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static io.undertow.Handlers.websocket;

public class SparkCodeSubmissionDriverPlugin implements org.apache.spark.api.plugin.DriverPlugin {
    static Logger logger = LoggerFactory.getLogger(SparkCodeSubmissionDriverPlugin.class);
    static final int PY4J_PORT = 9441;
    static final int RBACKEND_PORT = 9602;
    static final int FLIGHT_SQL_PORT = 33333;
    Undertow codeSubmissionServer;
    int port;
    ExecutorService virtualThreads;
    Py4JServer py4jServer;
    int pyport;
    String secret;
    RBackend rBackend;
    int rbackendPort;
    String rbackendSecret;
    Path pythonPath;
    Map<Integer,Integer> portMap = new ConcurrentHashMap<>();
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    boolean done = false;
    List<String> costList = new ArrayList<>();
    Path workDir = Path.of("/opt/spark/work-dir");
    Set<Integer> occupiedPorts = new HashSet<>();

    public SparkCodeSubmissionDriverPlugin() {
        this(-1);
    }

    public SparkCodeSubmissionDriverPlugin(int port) {
        this.port = port;
        virtualThreads = Executors.newFixedThreadPool(10);
    }

    public int getPort() {
        return port;
    }

    private Row initPy4JServer(SparkContext sc) throws IOException, NoSuchFieldException, IllegalAccessException {
        try {
            pythonPath = alterPysparkInitializeContext();
        } catch (IOException e) {
            try {
                pythonPath = alterSecondaryPysparkInitializeContext();
            } catch (IOException ex) {
                logger.error("Failed to alter pyspark initialize context", e);
            }
        }

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

            portMap.put(PY4J_PORT, pyport);

            Map<String, String> sysenv = System.getenv();
            Field field = sysenv.getClass().getDeclaredField("m");
            field.setAccessible(true);
            var env = ((Map<String, String>) field.get(sysenv));
            env.put("PYSPARK_GATEWAY_PORT", Integer.toString(pyport));
            env.put("PYSPARK_GATEWAY_SECRET", secret);
            env.put("PYSPARK_PIN_THREAD", "true");

            System.err.println("PYSPARK_GATEWAY_PORT: " + pyport);
            System.err.println("PYSPARK_GATEWAY_SECRET: " + secret);
        }
        return RowFactory.create("py4j", pyport, secret);
    }

    private Row initRBackend() {
        rBackend = new RBackend();
        Tuple2<Object, RAuthHelper> tuple = rBackend.init();
        rbackendPort = (Integer) tuple._1;
        rbackendSecret = tuple._2.secret();

        portMap.put(RBACKEND_PORT, rbackendPort);

        virtualThreads.submit(() -> {
            try {
                rBackend.run();
            } catch (Exception e) {
                logger.error("Failed to run RBackend", e);
            }
        });
        System.err.println("RBACKEND_PORT: " + rbackendPort);
        System.err.println("RBACKEND_SECRET: " + rbackendSecret);
        return RowFactory.create("rbackend", rbackendPort, rbackendSecret);
    }

    Process runProcess(List<String> arguments, Map<String,String> environment, String processName) throws IOException {
        return runProcess(arguments, environment, processName, true, null);
    }

    private Process runProcess(List<String> arguments, Map<String,String> environment, String processName, boolean inheritOutput, Path dir) throws IOException {
        var args = new ArrayList<>(Collections.singleton(processName));
        args.addAll(arguments);
        var pb  = new ProcessBuilder(args);
        if (dir!=null) pb.directory(dir.toFile());
        //pb.redirectErrorStream(true);
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
        return runProcess(args, pythonEnv, cmd, inheritOutput, null);
    }

    void gitCloneUrl(String checkoutGit) throws GitAPIException {
        var uri = URI.create(checkoutGit);
        var list = Arrays.stream(uri.getPath().split("/")).filter(s -> !s.isEmpty()).collect(Collectors.toList());
        var repoName = list.get(list.size() - 1);
        var repoDir = workDir.resolve(repoName);
        logger.info("Checking out git repo " + checkoutGit + " to " + repoDir);
        Git.cloneRepository().setURI(checkoutGit).setDirectory(repoDir.toFile()).call();
    }

    public String submitCode(SparkSession sqlContext, CodeSubmission codeSubmission, ObjectMapper mapper) throws IOException, ClassNotFoundException, NoSuchMethodException, URISyntaxException, ExecutionException, InterruptedException, GitAPIException, ApiException, SQLException {
        var defaultResponse = codeSubmission.type() + " code submitted";
        switch (codeSubmission.type()) {
            case SERVICE:
                var service = codeSubmission.code();
                if (service.equalsIgnoreCase("flightsql")) {
                    startFlightServer(sqlContext);
                }
                break;
            case NOTEBOOK:
                var notebook = codeSubmission.code();
                var port = codeSubmission.arguments.size() > 0 ? codeSubmission.arguments.get(0) : "";
                if (notebook.equalsIgnoreCase("JUPYTER")) {
                    startCodeJupyter(workDir, port);
                } else {
                    startCodeServer(workDir, port);
                }
                break;
            case GIT:
                var gitcmd = codeSubmission.code();
                if (gitcmd.equalsIgnoreCase("clone")) {
                    var checkoutGit = codeSubmission.arguments.get(0);
                    gitCloneUrl(checkoutGit);
                }
                break;
            case SQL:
                var sqlCode = codeSubmission.code();
                var resultsPath = codeSubmission.resultsPath();
                if (resultsPath == null || resultsPath.isEmpty()) {
                    if (sqlContext==null) {
                        var code = "import sys\n" +
                                "from pyspark.sql import SparkSession\n" +
                                "spark = SparkSession.builder.getOrCreate()\n" +
                                "json = spark.sql(\"%s\").toPandas().to_json()\n" +
                                "print(json)\n";
                        var pythonCode = String.format(code, sqlCode);
                        System.err.println("about to run " + pythonCode);
                        var process = runPython(pythonCode, codeSubmission.arguments(), codeSubmission.environment(), false);
                        defaultResponse = new String(process.getInputStream().readAllBytes());
                    } else {
                        defaultResponse = virtualThreads.submit(() -> {
                            var df = sqlContext.sql(sqlCode);
                            switch (codeSubmission.resultFormat()) {
                                case "json":
                                case "JSON":
                                    var json = df.toJSON().collectAsList();
                                    return mapper.writeValueAsString(json);
                                case "csv":
                                case "CSV":
                                    return String.join(",", df.collectAsList().toArray(String[]::new));
                                default:
                                    var rows = df.collectAsList();
                                    return mapper.writeValueAsString(rows);
                            }
                        }).get();
                    }
                } else {
                    if (sqlContext==null) {
                        var code = "import sys\n" +
                                "from pyspark.sql import SparkSession\n" +
                                "spark = SparkSession.builder.getOrCreate()\n" +
                                "spark.sql(\"%s\").write.format(\"%s\").mode(\"overwrite\").save(\"%s\")";
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
                break;
            case PYTHON:
                runPython(codeSubmission.code(), codeSubmission.arguments(), codeSubmission.environment());
                break;
            case PYTHON_BASE64:
                var pythonCodeBase64 = codeSubmission.code();
                var pythonCode = new String(Base64.getDecoder().decode(pythonCodeBase64));
                runPython(pythonCode, codeSubmission.arguments(), codeSubmission.environment());
                break;
            case R:
                virtualThreads.submit(() -> {
                    try {
                        var processName = "Rscript";
                        var RProcess = runProcess(codeSubmission.arguments(), codeSubmission.environment(), processName);
                        RProcess.getOutputStream().write(codeSubmission.code().getBytes());
                    } catch (IOException e) {
                        logger.error("R Execution failed: ", e);
                    }
                });
                break;
            case JAVA:
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
                break;
            case COMMAND:
                var processName = codeSubmission.code();
                /*args.add("console");
                args.add("--kernel");
                args.add("sparkmagic_kernels.pysparkkernel");
                args.add("--existing");
                args.add("kernel-" + pyport + ".json");*/
                runProcess(codeSubmission.arguments(), codeSubmission.environment(), processName);
                break;
            case COST:
                var cost = codeSubmission.code();
                logger.info("Adding cost" + cost);
                costList.add(cost);
                sqlContext.createDataset(costList, Encoders.STRING()).createOrReplaceGlobalTempView("cost");
                break;
            default:
                logger.error("Unknown code type: "+codeSubmission.type());
                break;
        }
        return defaultResponse;
    }

    private boolean fixContext(Path pysparkPath) {
        if (Files.exists(pysparkPath)) {
            try {
                var oldStatement = "return self._jvm.JavaSparkContext(jconf)";
                var pyspark = Files.readString(pysparkPath);
                if (pyspark.contains(oldStatement)) {
                    pyspark = pyspark.replace(oldStatement, "return self._jvm.JavaSparkContext.fromSparkContext(self._jvm.org.apache.spark.SparkContext.getOrCreate(jconf))");
                    Files.writeString(pysparkPath, pyspark);
                    logger.info("Pyspark initialize context altered");
                    return true;
                }
            } catch (IOException e) {
                logger.error("Failed to alter pyspark initialize context", e);
            }
        }
        return false;
    }

    private Path alterPysparkInitializeContext() throws IOException {
        var sparkHome = System.getenv("SPARK_HOME");
        if (sparkHome != null) {
            var workDir = Path.of(sparkHome).resolve("python");
            var pysparkPath = workDir.resolve("pyspark").resolve("context.py");
            if (fixContext(pysparkPath)) return workDir;
        }
        var pysparkPython = System.getenv("PYSPARK_PYTHON");
        var cmd = pysparkPython != null ? pysparkPython : "python3";
        var processBuilder = new ProcessBuilder(cmd, "-c", "import pyspark, os; print(os.path.dirname(pyspark.__file__))");
        var process = processBuilder.start();
        var pathStr = new String(process.getInputStream().readAllBytes());
        var path = Path.of(pathStr.trim());
        var pysparkPath = path.resolve("context.py");
        if (fixContext(pysparkPath)) return path.getParent();
        else return alterSecondaryPysparkInitializeContext();
    }

    void unzip(Path zipFilePath, Path destDirectory) throws IOException {
        try (ZipInputStream zipIn = new ZipInputStream(Files.newInputStream(zipFilePath))) {
            ZipEntry entry = zipIn.getNextEntry();
            while (entry != null) {
                Path of = destDirectory.resolve(entry.getName());
                if (!entry.isDirectory()) {
                    Files.copy(zipIn, of, StandardCopyOption.REPLACE_EXISTING);
                } else {
                    Files.createDirectories(of);
                }
                zipIn.closeEntry();
                entry = zipIn.getNextEntry();
            }
        }
    }

    private Path alterSecondaryPysparkInitializeContext() throws IOException {
        var workDir = Path.of("/opt/spark/work-dir/python");
        Files.createDirectories(workDir);
        unzip(Path.of("/opt/spark/python/lib/pyspark.zip"), workDir);
        var pysparkPath = workDir.resolve("pyspark").resolve("context.py");
        fixContext(pysparkPath);
        return workDir;
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
                                //ToreeLauncher.main(argumentsList.toArray(String[]::new));
                                /*var args = new ArrayList<String>();
                                args.add("src/main/resources/launch_ipykernel.py");
                                args.addAll(argumentsList);
                                runProcess(args, Collections.emptyMap(), "python3");*/
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
                         InterruptedException | NoSuchMethodException | GitAPIException | ApiException | SQLException e) {
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

    void handleWebsocketRequest(WebSocketHttpExchange exchange, WebSocketChannel channel) {
        /*if (exchange.getRequestParameters().containsKey("grpc")) {
            var grpcList = exchange.getRequestParameters().get("grpc");
            if (grpcList.size() > 0) {
                grpcPort = Integer.parseInt(grpcList.get(0));
            }
        }
        if (exchange.getRequestParameters().containsKey("hive")) {
            var hiveList = exchange.getRequestParameters().get("hive");
            if (hiveList.size() > 0) {
                hivePort = Integer.parseInt(hiveList.get(0));
            }
        }*/

        try {
            var sparkReceiveListener = new SparkReceiveListener(virtualThreads, channel, portMap);
            channel.getReceiveSetter().set(sparkReceiveListener);
            channel.resumeReceives();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void startCodeSubmissionServer(SparkContext sc) throws IOException {
        var mapper = new ObjectMapper();
        var webSocketPostHandler = websocket(this::handleWebsocketRequest, new BlockingHandler(exchange -> handlePostRequest(exchange, sc, mapper)));
        codeSubmissionServer = Undertow.builder()
            .addHttpListener(port, "0.0.0.0")
            .setHandler(webSocketPostHandler)
            .build();

        System.err.println("Using submission port "+ port);
        codeSubmissionServer.start();
    }

    void startWebsockify(Path workDir) throws IOException, InterruptedException {
        var installDir = workDir.resolve("websockify");
        runProcess(List.of(
                "install",
                "--target",
                installDir.toString(),
                "websockify"), Map.of(), "pip", true, null).waitFor();

    }

    public void untar(URL url, Path workDir, boolean gz) throws IOException {
        try (var gzip = gz ? new GZIPInputStream(url.openStream()) : new XZCompressorInputStream(url.openStream()); var tar = new TarArchiveInputStream(gzip)) {
            var entry = tar.getNextEntry();
            while (entry != null) {
                var file = workDir.resolve(entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectories(file);
                } else {
                    Files.copy(tar, file, StandardCopyOption.REPLACE_EXISTING);
                }
                entry = tar.getNextEntry();
            }
        }
    }

    void startCodeTunnel(Path workDir) throws IOException {
        var url = new URL("https://code.visualstudio.com/sha/download?build=stable&os=cli-alpine-x64");
        untar(url, workDir, true);
        var code = Path.of("code");
        var codePath = workDir.resolve(code);
        Files.setPosixFilePermissions(codePath, PosixFilePermissions.fromString("rwxr-xr-x"));
        runProcess(List.of("tunnel", "--cli-data-dir", workDir.toString(), "--accept-server-license-terms"), Collections.emptyMap(), codePath.toString(), true, null);
    }

    void startCodeServer(Path workDir) throws IOException, InterruptedException {
        startCodeServer(workDir, "");
    }

    void startCodeServer(Path workDir, String codeServerPort) throws IOException, InterruptedException {
        int port = 8080;
        try {
            port = Integer.parseInt(codeServerPort);
        } catch (NumberFormatException e) {
            logger.info("No default code server port: " + codeServerPort, e);
        }

        var url = URI.create("https://code-server.dev/install.sh");
        var path = workDir.resolve("install.sh");
        var codeserver = workDir.resolve("bin").resolve("code-server");
        var extensions = workDir.resolve("extensions");
        try (var in = url.toURL().openStream()) {
            Files.copy(in, path, StandardCopyOption.REPLACE_EXISTING);
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwxr-xr-x"));
            System.err.println("installing");
            runProcess(List.of("--version", "4.13.0", "--method", "standalone", "--prefix", workDir.toString()), Map.of("XDG_CACHE_HOME", workDir.resolve(".cache").toString()), path.toString(), true, null).waitFor();
            System.err.println("installing extensions");
            runProcess(List.of("--install-extension", "ms-python.python", "--extensions-dir", extensions.toString(), "--auth", "none"), Map.of("HOME", workDir.toString()), codeserver.toString(), true, null).waitFor();
            runProcess(List.of("--install-extension", "ms-toolsai.jupyter", "--extensions-dir", extensions.toString(), "--auth", "none"), Map.of("HOME", workDir.toString()), codeserver.toString(), true, null).waitFor();
            runProcess(List.of("--install-extension", "ms-toolsai.jupyter-keymap", "--extensions-dir", extensions.toString(), "--auth", "none"), Map.of("HOME", workDir.toString()), codeserver.toString(), true, null).waitFor();
            runProcess(List.of("--install-extension", "ms-toolsai.jupyter-renderers", "--extensions-dir", extensions.toString(), "--auth", "none"), Map.of("HOME", workDir.toString()), codeserver.toString(), true, null).waitFor();
            System.err.println("starting");
            runProcess(List.of("--auth", "none", "--bind-addr", "0.0.0.0:"+port, "--user-data-dir", workDir.toString(), "--extensions-dir", extensions.toString()),
                    Map.of(
                            "HOME", workDir.toString(),
                            "PYTHONPATH", "/opt/spark/python/lib/py4j-0.10.9.7-src.zip" + (pythonPath!=null?":"+pythonPath:""),
                            "SPARK_HOME", "/opt/spark"),
                    codeserver.toString(),
                    true, null);
        }
    }

    private static Optional<String> getJupyterBaseUrl() {
        var baseurl = System.getenv("JUPYTER_BASE_URL");
        if (baseurl == null) {
            baseurl = System.getProperty("JUPYTER_BASE_URL");
        }
        if (baseurl != null && !baseurl.isEmpty()) {
            return Optional.of(baseurl);
        }
        return Optional.empty();
    }

    String getClusterId(String appName) throws IOException, ApiException {
        var client = Config.defaultClient();
        //client.setBasePath("https://your-kubernetes-cluster-url");
        var api = new CoreV1Api(client);

        System.err.println("Read pod "+appName);
        var pod = api.readNamespacedPod(appName+"-driver", "spark-apps", null);
        var labels = Objects.requireNonNull(pod.getMetadata())
                .getLabels();
        assert labels != null;
        return labels.get("bigdata.spot.io/cluster-id");
    }

    String getClusterIdFabric8(String appName) {
        String namespace = "spark-apps";
        String podName = appName+"-driver";

        var kubernetesClientBuilder = new KubernetesClientBuilder();
        try (KubernetesClient client = kubernetesClientBuilder.build()) {
            Pod pod = client.pods().inNamespace(namespace).withName(podName).get();
            assert pod != null;
            var labels = Objects.requireNonNull(pod.getMetadata()).getLabels();
            return labels.get("bigdata.spot.io/cluster-id");
        }
    }

    void startCodeJupyter(Path workDir) throws IOException, ApiException, InterruptedException {
        startCodeJupyter(workDir, "");
    }

    void startCodeJupyter(Path workDir, String jupyterServerPort) throws IOException, ApiException, InterruptedException {
        logger.info("Install JupyterLab");
        int port = 8888;
        try {
            port = Integer.parseInt(jupyterServerPort);
        } catch (NumberFormatException e) {
            logger.info("No default code server port: " + jupyterServerPort, e);
        }

        var hostName = InetAddress.getLocalHost().getHostName();
        logger.info("Starting jupyter server for host: " + hostName);
        var appName = hostName.substring(0,hostName.lastIndexOf('-'));

        var installDir = workDir.resolve("jupyter");
        runProcess(List.of(
                "install",
                "--target",
                installDir.toString(),
                "jupyterlab", "jupyter_server"), Map.of(), "pip", true, null).waitFor();
        var jupyter = installDir.resolve("bin").resolve("jupyter");
        var url = "https://nodejs.org/dist/v18.16.0/node-v18.16.0-linux-x64.tar.xz";
        var uri = URI.create(url);
        untar(uri.toURL(), workDir, false);
        var nodepath = workDir.resolve("node-v18.16.0-linux-x64").resolve("bin");
        try (var flist = Files.list(nodepath)) {
            flist.forEach(p -> {
                try {
                    Files.setPosixFilePermissions(p, PosixFilePermissions.fromString("rwxr-xr-x"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        var pathvar = System.getenv("PATH")+":"+nodepath;
        System.err.println("Using pathvar " + pathvar);
        var appDir = workDir.resolve("lab");
        runProcess(List.of(
                    "lab",
                    "build",
                    "--app-dir="+appDir,
                    "--dev-build=False",
                    "--minimize=False"),
                Map.of( "PATH", pathvar,
                        "HOME", workDir.toString(),
                        "PYTHONPATH", installDir.toString()), jupyter.toString(), true, null).waitFor();

        var clusterId = getClusterIdFabric8(appName);

        System.err.println("Starting JupyterLab");
        var plist = List.of("lab", "--ip=0.0.0.0", "--NotebookApp.allow_origin=*", "--port="+port, "--NotebookApp.disable_check_xsrf=True", "--NotebookApp.port_retries=0",
                        //"--ServerApp.base_url=/proxy/8889",
                        //"--ServerApp.base_url=/apps/"+appName+"/notebook",
                        "--app-dir="+appDir,
                        "--ServerApp.base_url=/api/ocean/spark/cluster/"+clusterId+"/app/"+appName+"/notebook",
                        "--NotebookApp.token=''",
                        "--no-browser",
                        "--notebook-dir=" + workDir); //, "--NotebookApp.token","''","--NotebookApp.disable_check_xsrf","True"));
        runProcess(plist, Map.of(
                "HOME", workDir.toString(),
                "PYTHONPATH", installDir.toString(),
                "SPARK_HOME", "/opt/spark",
                "JUPYTER_CONFIG_DIR",
                workDir.toString(),
                "JUPYTER_DATA_DIR",
                workDir.toString(),
                "JUPYTER_RUNTIME_DIR",
                workDir.toString()), jupyter.toString(), true, null);
    }

    /*void stuff() {
        int pyPort = connectInfo.stream().filter(r -> r.getString(0).equals("py4j")).map(r -> r.getInt(1)).findFirst().orElseThrow();
                var pythonPath = Path.of("/opt/spark/python");
                var pythonPathEnv = System.getenv("PYTHONPATH");
                if (pythonPathEnv != null && !pythonPathEnv.isEmpty()) {
                    pythonPath = pythonPath.resolve(pythonPathEnv);
                }
                var pythonPathStr = pythonPath.toString();
                logger.info("Forwarding PYTHONPATH: " + pythonPathStr);
                sc.conf().set("spark.executorEnv.PYTHONPATH", pythonPathStr);
                sc.conf().set("spark.yarn.appMasterEnv.PYTHONPATH", pythonPathStr);
                sc.conf().set("spark.driverEnv.PYTHONPATH", pythonPathStr);
                var grpcPort = Integer.parseInt(sc.conf().get("spark.connect.grpc.binding.port", "15002"));
                var hivePortStr = System.getenv("HIVE_SERVER2_THRIFT_PORT");
                var hivePort = Integer.parseInt((hivePortStr == null || hivePortStr.isEmpty()) ? "10000" : hivePortStr);
    }*/

    void startFlightServer(SparkSession sparkSession) throws IOException, SQLException {
        if (occupiedPorts.contains(FLIGHT_SQL_PORT)) {
            logger.info("Flight server already started");
            return;
        } else occupiedPorts.add(FLIGHT_SQL_PORT);
        var location = Location.forGrpcInsecure("0.0.0.0", FLIGHT_SQL_PORT);
        var allocator = new RootAllocator();
        var flightServer = FlightServer.builder(allocator, location, new SparkSQLProducer(sparkSession)).build();
        flightServer.start();
        System.out.println("S1: Server (Location): Listening on port " + flightServer.getPort());
        //flightServer.awaitTermination();
    }

    void initConnections(SparkSession sparkSession, boolean useHive, boolean useFlight, List<Row> connectInfo) throws IOException, SQLException {
        if (useHive) {
            var hiveThriftServer = new HiveThriftServer2(sparkSession.sqlContext());
            var hiveEventManager = new HiveThriftServer2EventManager(sparkSession.sparkContext());
            HiveThriftServer2.eventManager_$eq(hiveEventManager);
            var hiveConf = new HiveConf();
            hiveThriftServer.init(hiveConf);
            //hiveThriftServer.startWithContext(sparkSession.sqlContext());
            hiveThriftServer.start();

            /*var hivePort = hiveThriftServer.getHiveServer2Port();
            var hivePortStr = System.getenv("HIVE_SERVER2_THRIFT_PORT");
            if (hivePortStr == null || hivePortStr.isEmpty()) {
                System.err.println("HIVE_SERVER2_THRIFT_PORT not set, using " + hivePort);
                hivePortStr = Integer.toString(hivePort);
            }
            var hiveHost = System.getenv("HIVE_SERVER2_THRIFT_BIND_HOST");
            if (hiveHost == null || hiveHost.isEmpty()) {
                System.err.println("HIVE_SERVER2_THRIFT_BIND_HOST not set, using*/
        }

        if (useFlight) {
           startFlightServer(sparkSession);
        }

        var df = sparkSession.createDataset(connectInfo, RowEncoder.apply(StructType.fromDDL("type string, port int, secret string")));
        df.createOrReplaceGlobalTempView("spark_connect_info");
        var connInfoPath = System.getenv("PYSPARK_DRIVER_CONN_INFO_PATH");
        if (connInfoPath != null && !connInfoPath.isEmpty()) df.write().mode(SaveMode.Overwrite).save(connInfoPath);
    }

    void init(SparkContext sc, SparkSession sparkSession) throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        logger.info("Starting code submission server");
        if (port == -1) {
            port = Integer.parseInt(sc.conf().get("spark.code.submission.port", "9001"));
        }
        try {
            var portMapConf = sc.conf().get("spark.code.port.map", "");
            if (!portMapConf.isEmpty()) {
                for (var entry : portMapConf.split(",")) {
                    var parts = entry.split(":");
                    portMap.put(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
                }
            }
            var checkoutGit = sc.conf().get("spark.code.git.checkout", "");
            if (!checkoutGit.isEmpty()) {
                gitCloneUrl(checkoutGit);
            }

            var useSparkConnect =sc.conf().get("spark.code.submission.connect", "true");
            var usePySpark = sc.conf().get("spark.code.submission.pyspark", "true");
            var useRBackend = sc.conf().get("spark.code.submission.sparkr", "true");
            var useCodeTunnel = sc.conf().get("spark.code.tunnel", "false");
            var useCodeServer = sc.conf().get("spark.code.server.port", "");
            var useJupyterServer = sc.conf().get("spark.code.jupyter.port", "");
            var useHive = sc.conf().get("spark.code.submission.hive", "true");
            var useFlight = sc.conf().get("spark.code.submission.flight", "true");
            var useWebsockify = sc.conf().get("spark.code.submission.websockify", "false");
            if (useSparkConnect.equalsIgnoreCase("true")) SparkConnectService.start();

            var connectInfo = new ArrayList<Row>();
            if (usePySpark.equalsIgnoreCase("true")) connectInfo.add(initPy4JServer(sc));
            if (useRBackend.equalsIgnoreCase("true")) connectInfo.add(initRBackend());
            connectInfo.forEach(row -> logger.info("Connect info: " + row));
            connectInfo.forEach(row -> System.err.println("Connect info: " + row));

            if (useCodeServer.length()>0) {
                virtualThreads.submit(() -> {
                    try {
                        startCodeServer(workDir, useCodeServer);
                    } catch (IOException | InterruptedException e) {
                        logger.error("Error starting code server", e);
                    }
                });
            }
            if (useJupyterServer.length()>0) {
                //var applicationId = sc.applicationId();
                //var appName = sc.appName();
                //var appId = sc.conf().getAppId();
                //logger.info("Starting jupyter server for application: " + applicationId + " " + appName + " " + appId);
                virtualThreads.submit(() -> {
                    try {
                        startCodeJupyter(workDir, useJupyterServer);
                    } catch (IOException | ApiException | InterruptedException e) {
                        logger.error("Unable to start jupyter server", e);
                    }
                });
            }
            if (useCodeTunnel.equalsIgnoreCase("true")) startCodeTunnel(workDir);

            if (sparkSession != null) {
                initConnections(sparkSession, useHive.equalsIgnoreCase("true"), useFlight.equalsIgnoreCase("true"), connectInfo);
            } else {
                var session = SparkSession.getDefaultSession();
                if (session.isDefined()) {
                    initConnections(session.get(), useHive.equalsIgnoreCase("true"), useFlight.equalsIgnoreCase("true"), connectInfo);
                } else {
                    virtualThreads.submit(() -> {
                        var inSession = session;
                        while (inSession.isEmpty() && !done) {
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException e) {
                                logger.error("Interrupted", e);
                                return;
                            }
                            inSession = SparkSession.getDefaultSession();
                        }
                        if (!done) {
                            try {
                                initConnections(inSession.get(), useHive.equalsIgnoreCase("true"), useFlight.equalsIgnoreCase("true"), connectInfo);
                            } catch (IOException | SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                }
            }
            //stuff();
            if (useWebsockify.equalsIgnoreCase("true")) startWebsockify(workDir);
            else startCodeSubmissionServer(sc);
        } catch (RuntimeException e) {
            logger.error("Failed to start code submission server at port: " + port, e);
            throw e;
        } catch (IOException e) {
            logger.error("Unable to alter pyspark context code", e);
            throw new RuntimeException(e);
        } catch (GitAPIException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            logger.error("SQLException", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String,String> init(SparkContext sc, PluginContext myContext) {
        try {
            init(sc, (SparkSession)null);
        } catch (NoSuchFieldException | IllegalAccessException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        return Collections.emptyMap();
    }

    @Override
    public void shutdown() {
        done = true;
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
