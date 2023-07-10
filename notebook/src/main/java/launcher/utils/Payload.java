package launcher.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Payload {
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getConn_info() {
        return conn_info;
    }

    public void setConn_info(String conn_info) {
        this.conn_info = conn_info;
    }

    String key;
    String conn_info;

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    int version = 1;

    public Payload(String key, String conn_info) {
        this.key = key;
        this.conn_info = conn_info;
    }

    public static String createJson(String key, String conn_info) throws JsonProcessingException {
        var newPayload = new Payload(key, conn_info);
        return new ObjectMapper().writeValueAsString(newPayload);
    }
}
