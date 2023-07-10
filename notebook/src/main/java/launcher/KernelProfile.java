package launcher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import launcher.utils.SocketUtils;

import java.util.UUID;

public class KernelProfile {
    public int getHb_port() {
        return hb_port;
    }

    public void setHb_port(int hb_port) {
        this.hb_port = hb_port;
    }

    public int getControl_port() {
        return control_port;
    }

    public void setControl_port(int control_port) {
        this.control_port = control_port;
    }

    public int getIopub_port() {
        return iopub_port;
    }

    public void setIopub_port(int iopub_port) {
        this.iopub_port = iopub_port;
    }

    public int getStdin_port() {
        return stdin_port;
    }

    public void setStdin_port(int stdin_port) {
        this.stdin_port = stdin_port;
    }

    public int getShell_port() {
        return shell_port;
    }

    public void setShell_port(int shell_port) {
        this.shell_port = shell_port;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKernel_name() {
        return kernel_name;
    }

    public void setKernel_name(String kernel_name) {
        this.kernel_name = kernel_name;
    }

    public String getSignature_scheme() {
        return signature_scheme;
    }

    public void setSignature_scheme(String signature_scheme) {
        this.signature_scheme = signature_scheme;
    }

    public String getTransport() {
        return transport;
    }

    public void setTransport(String transport) {
        this.transport = transport;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    private int hb_port;
    private int control_port;
    private int iopub_port;
    private int stdin_port;
    private int shell_port;
    private String key;
    private String kernel_name;
    private String signature_scheme;
    private String transport;
    private String ip;

    public KernelProfile(int hb_port, int control_port, int iopub_port, int stdin_port, int shell_port, String key,
                         String kernel_name, String signature_scheme, String transport, String ip) {
        this.hb_port = hb_port;
        this.control_port = control_port;
        this.iopub_port = iopub_port;
        this.stdin_port = stdin_port;
        this.shell_port = shell_port;
        this.key = key;
        this.kernel_name = kernel_name;
        this.signature_scheme = signature_scheme;
        this.transport = transport;
        this.ip = ip;
    }

    public static String newKey() {
        return UUID.randomUUID().toString();
    }

    public static String createJsonProfile(int portLowerBound, int portUpperBound) throws JsonProcessingException {
        var mapper = new ObjectMapper();
        var newKernelProfile = new KernelProfile(
                SocketUtils.findPort(portLowerBound, portUpperBound),
                SocketUtils.findPort(portLowerBound, portUpperBound),
                SocketUtils.findPort(portLowerBound, portUpperBound),
                SocketUtils.findPort(portLowerBound, portUpperBound),
                SocketUtils.findPort(portLowerBound, portUpperBound),
                newKey(),
                "Apache Toree Scala", "hmac-sha256", "tcp", "0.0.0.0"
        );
        return mapper.writeValueAsString(newKernelProfile);
    }
}