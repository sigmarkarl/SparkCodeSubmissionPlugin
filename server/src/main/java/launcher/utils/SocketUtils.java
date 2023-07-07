package launcher.utils;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

//import org.apache.toree.utils.LogLike;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketUtils /*implements LogLike*/ {
    static Logger logger = LoggerFactory.getLogger(SocketUtils.class);

    private static final Random random = new Random(System.currentTimeMillis());

    public static void writeToSocket(String socketAddress, String content) {
        String[] ipPort = socketAddress.split(":");
        if (ipPort.length == 2) {
            logger.info("Sending connection info to gateway at {}\n{}", socketAddress, content);
            String ip = ipPort[0];
            int port = Integer.parseInt(ipPort[1]);
            try (Socket socket = new Socket(InetAddress.getByName(ip), port);
                 PrintStream out = new PrintStream(socket.getOutputStream())) {
                out.append(content);
                out.flush();
            } catch (IOException e) {
                logger.error("Error while writing to socket: " + e.getMessage(), e);
            }
        } else {
            logger.error("Invalid format for response address '{}'", socketAddress);
        }
    }

    public static int findPort(int portLowerBound, int portUpperBound) {
        try (ServerSocket socket = findSocket(portLowerBound, portUpperBound)) {
            int port = socket.getLocalPort();
            logger.info("Port {} is available", port);
            return port;
        } catch (IOException e) {
            logger.error("Error while finding available port: " + e.getMessage(), e);
            return -1;
        }
    }

    public static ServerSocket findSocket(int portLowerBound, int portUpperBound) throws IOException {
        boolean foundAvailable = false;
        ServerSocket socket = null;

        while (!foundAvailable) {
            int candidatePort = getCandidatePort(portLowerBound, portUpperBound);
            logger.info("Trying port {} ...", candidatePort);

            try {
                socket = new ServerSocket(candidatePort);
                foundAvailable = true;
            } catch (IOException e) {
                logger.info("Port {} is in use", candidatePort);
            }
        }

        return socket;
    }

    private static int getCandidatePort(int portLowerBound, int portUpperBound) {
        int portRange = portUpperBound - portLowerBound;
        if (portRange <= 0) {
            return 0;
        }

        return portLowerBound + random.nextInt(portRange);
    }
}