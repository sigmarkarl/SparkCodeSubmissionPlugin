package launcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import launcher.utils.SecurityUtils;
import launcher.utils.SocketUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ToreeLauncher {
    static Logger logger = LoggerFactory.getLogger(ToreeLauncher.class);
    private static final int minPortRangeSize = Integer.parseInt(
            System.getenv().getOrDefault(
                    "MIN_PORT_RANGE_SIZE",
                    System.getenv().getOrDefault("EG_MIN_PORT_RANGE_SIZE", "1000")
            )
    );
    private static final String kernelTempDir = "jupyter-kernel";
    private static String profilePath;
    private static String kernelId;
    private static int portLowerBound = -1;
    private static int portUpperBound = -1;
    private static String responseAddress;
    private static String publicKey;
    private static String alternateSigint;
    private static String initMode = "lazy";
    private static List<String> toreeArgs;

    private static boolean pathExists(String filePath) {
        if (filePath == null) {
            return false;
        }
        return Files.exists(Paths.get(filePath));
    }

    private static void writeToFile(String outputPath, String content) throws IOException {
        File file = new File(outputPath);
        if (!pathExists(file.getParentFile().toString())) {
            file.getParentFile().mkdirs(); // mkdir if not exists
        }
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            bw.write(content);
        }
    }

    private static void initPortRange(String portRange) {
        String[] ports = portRange.split("\\.\\.");

        portLowerBound = Integer.parseInt(ports[0]);
        portUpperBound = Integer.parseInt(ports[1]);

        System.out.println(
                "Port Range: lower bound ( " + portLowerBound + " ) / upper bound ( " + portUpperBound + " )"
        );

        if (portLowerBound != portUpperBound) {
            // Range of zero disables port restrictions
            if (portLowerBound < 0 || portUpperBound < 0 || (portUpperBound - portLowerBound < minPortRangeSize)) {
                System.err.println(
                        "Invalid port range, use --port-range <LowerBound>..<UpperBound>, " +
                                "range must be >= MIN_PORT_RANGE_SIZE (" + minPortRangeSize + ")"
                );
                System.exit(-1);
            }
        }
    }

    private static void initArguments(String[] args) {
        System.out.println("Toree launcher arguments (initial):");
        for (String arg : args) {
            System.out.println(arg);
        }
        System.out.println("---------------------------");

        // Walk the arguments, collecting launcher options along the way and buildup a
        // new toree arguments list.  There's got to be a better way to do this.
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            switch (arg) {
                // Profile is a straight pass-thru to toree
                case "--profile":
                    i++;
                    profilePath = args[i].trim();
                    toreeArgs.add(arg);
                    toreeArgs.add(profilePath);
                    break;

                // Alternate sigint is a straight pass-thru to toree
                case "--alternate-sigint":
                    i++;
                    alternateSigint = args[i].trim();
                    toreeArgs.add(arg);
                    toreeArgs.add(alternateSigint);
                    break;

                // Initialization mode requires massaging for toree
                case "--spark-context-initialization-mode":
                case "--RemoteProcessProxy.spark-context-initialization-mode":
                    i += 1;
                    initMode = args[i].trim();
                    if (initMode.equals("none")) {
                        toreeArgs.add("--nosparkcontext");
                    } else {
                        toreeArgs.add("--spark-context-initialization-mode");
                        toreeArgs.add(initMode);
                    }
                    break;

                // Port range doesn't apply to toree, consume here
                case "--port-range":
                case "--RemoteProcessProxy.port-range":
                    i += 1;
                    initPortRange(args[i].trim());
                    break;

                    // Response address doesn't apply to toree, consume here
                case "--response-address":
                case "--RemoteProcessProxy.response-address":
                    i += 1;
                    responseAddress = args[i].trim();
                    break;
                    // kernel id doesn't apply to toree, consume here
                case "--kernel-id":
                case "--RemoteProcessProxy.kernel-id":
                    i += 1;
                    kernelId = args[i].trim();
                    break;

                    // Public key doesn't apply to toree, consume here
                case "--public-key":
                case "--RemoteProcessProxy.public-key":
                    i += 1;
                    publicKey = args[i].trim();
                    break;

                    // All other arguments should pass-thru to toree
                default:
                    toreeArgs.add(args[i].trim());
            }
        }
    }

    // Borrowed from toree to avoid dependency
    private static void deleteDirRecur(File file) {
        // delete directory recursively
        if (file != null){
            if (file.isDirectory()){
                Arrays.stream(file.listFiles()).forEach(ToreeLauncher::deleteDirRecur);
            }
            if (file.exists()){
                file.delete();
            }
        }
    }

    private static String determineConnectionFile(String connectionFile, String kernelId) throws IOException {
        // We know the connection file does not exist, so create a temporary directory
        // and derive the filename from kernelId, if not null.
        // If kernelId is null, then use the filename in the connectionFile.

        var tmpPath = Files.createTempDirectory(kernelTempDir);
        // tmpPath.toFile.deleteOnExit() doesn't appear to work, use system hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteDirRecur(tmpPath.toFile())));
        var fileName = (kernelId != null) ? "kernel-" + kernelId + ".json" : Paths.get(connectionFile).getFileName().toString();
        var newPath = Paths.get(tmpPath.toString(), fileName);
        var newConnectionFile = newPath.toString();
        // Locate --profile and replace next element with new name.  If it doesn't exist, add both.
        var profileIndex = toreeArgs.indexOf("--profile");
        if (profileIndex >= 0) {
            toreeArgs.set(profileIndex + 1, newConnectionFile);
        } else {
            toreeArgs.add("--profile");
            toreeArgs.add(newConnectionFile);
        }

        return newConnectionFile;
    }

    private static String getPID() {
        // Return the current process ID. If not an integer string, server will ignore.
        return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    }

    private static ServerSocket initProfile(String[] args) throws IOException, NoSuchPaddingException, IllegalBlockSizeException, NoSuchAlgorithmException, BadPaddingException, InvalidKeySpecException, InvalidKeyException {
        ServerSocket commSocket = null;

        initArguments(args);

        if (profilePath == null && kernelId == null){
            logger.error("At least one of '--profile' or '--kernel-id' " +
                    "must be provided - exiting!");
            System.exit(-1);
        }

        if (kernelId == null) {
            logger.error("Parameter '--kernel-id' must be provided - exiting!");
            System.exit(-1);
        }

        if (publicKey == null) {
            logger.error("Parameter '--public-key' must be provided - exiting!");
            System.exit(-1);
        }

        if (!pathExists(profilePath)) {
            profilePath = determineConnectionFile(profilePath, kernelId);

            logger.info(String.format("%s saved", profilePath));

            var content = KernelProfile.createJsonProfile(portLowerBound, portUpperBound);
            writeToFile(profilePath, content);

            if (pathExists(profilePath)) {
                logger.info(String.format("%s saved", profilePath));
            } else {
                logger.error(String.format("Failed to create: %s", profilePath));
                System.exit(-1);
            }

            var mapper = new ObjectMapper();
            var connectionJson = (Map<String,Object>)mapper.readValue(content, Map.class);

            // Now need to also return the PID info in connection JSON
            connectionJson.put("pid", getPID());
            //connectionJson = connectionJson.as[JsObject] ++ Json.obj("pid" -> getPID());

            // Add kernelId
            connectionJson.put("kernel_id", kernelId);
            //connectionJson = connectionJson.as[JsObject] ++ Json.obj("kernel_id" -> kernelId)

            // Server wants to establish socket communication. Create socket and
            // convey port number back to the server.
            commSocket = SocketUtils.findSocket(portLowerBound, portUpperBound);
            connectionJson.put("comm_port", commSocket.getLocalPort());
            //connectionJson = connectionJson.as[JsObject] ++ Json.obj("comm_port" -> commSocket.getLocalPort());
            var jsonContent = mapper.writeValueAsString(connectionJson);

            if (responseAddress != null) {
                logger.info(String.format("JSON Payload: '%s'", (Object) jsonContent));
                var payload = SecurityUtils.encrypt(publicKey, jsonContent);
                logger.info(String.format("Encrypted Payload: '%s'", payload));
                SocketUtils.writeToSocket(responseAddress, payload);
            }
        }
        return commSocket;
    }

    private static String getServerRequest(ServerSocket commSocket) throws IOException {
        var s = commSocket.accept();
        var data = new String(s.getInputStream().readAllBytes());
        s.close();
        return data;
    }

    private static String getReconciledSignalName(int sigNum) {
        //assert(sigNum > 0, "sigNum must be greater than zero");

        if (sigNum == 9)
            return "TERM";
        else {
            if (alternateSigint == null) {
                logger.warn(String.format(
                        "--alternate-sigint is not defined and signum %d has been requested. Using SIGINT, " +
                                "which probably won't get received due to JVM preventing interrupts on background " +
                                "processes. Define --alternate-sigint using __TOREE_OPTS__.", sigNum));
                return "INT";
            } else {
                return alternateSigint;
            }
        }
    }

    private static void serverListener(ServerSocket commSocket) throws IOException {
        var objectMapper = new ObjectMapper();
        boolean stop = false;
        while (!stop) {
            String requestData = getServerRequest(commSocket);

            // Handle each of the requests. Note that we do not make an assumption that these are
            // mutually exclusive - although that will probably be the case for now. Over time,
            // this should probably get refactored into a) better scala and b) token/classes for
            // each request.

            var requestJson = objectMapper.readValue(requestData, Map.class);

            // Signal the kernel...
            if (requestJson.containsKey("signum")) {
                int sigNum = Integer.parseInt(requestJson.get("signum").toString());
                if (sigNum > 0) {
                    // If sigNum anything but 0 (for poll), use Signal.raise(signal) to signal the kernel.
                    String sigName = getReconciledSignalName(sigNum);
                    Signal sigToRaise = new Signal(sigName);
                    logger.info(String.format("Server listener raising signal: '%s' (%d) for signum: %d",
                            sigToRaise.getName(), sigToRaise.getNumber(), sigNum));
                    Signal.raise(sigToRaise);
                }
            }
            // Stop the listener...
            if (requestJson.containsKey("shutdown")) {
                int shutdown = Integer.parseInt(requestJson.get("shutdown").toString());
                if (shutdown == 1) {
                    // The server has been instructed to shutdown the kernel, so let's stop
                    // the listener so that it doesn't interfere with poll() calls.
                    logger.info("Stopping server listener.");
                    stop = true;
                }
            }
        }
    }

    public static void main(String[] args) {
        toreeArgs = new ArrayList<>();
        profilePath = null;
        kernelId = null;
        publicKey = null;
        try (var commSocket = initProfile(args)) {
            // if commSocket is not null, start a thread to listen on socket
            if (commSocket != null) {
                Thread serverListenerThread = new Thread(() -> {
                    try {
                        serverListener(commSocket);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                System.out.println("Starting server listener...");
                serverListenerThread.start();
            }

            System.out.println("Toree kernel arguments (final):");
            for (String arg : toreeArgs) {
                System.out.println(arg);
            }
            System.out.println("---------------------------");
            //Main.main(toreeArgs.toArray(String[]::new));
        } catch (IOException | NoSuchPaddingException | IllegalBlockSizeException | NoSuchAlgorithmException |
                 BadPaddingException | InvalidKeySpecException | InvalidKeyException e) {
            e.printStackTrace();
        }
    }
}

