package com.netapp.spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class NotebookInitContainer {
    public static void main(String[] args) throws IOException {
        try (var is = NotebookInitContainer.class.getResourceAsStream("/launch_ipykernel.py");
             var isold = NotebookInitContainer.class.getResourceAsStream("/launch_ipykernel_old.py");
             var iseg322 = NotebookInitContainer.class.getResourceAsStream("/launch_ipykernel_eg322.py")) {
            if (is != null && isold != null && iseg322 != null) {
                var path = Path.of("/opt/spark/work-dir/launch_ipykernel.py");
                var path_old = Path.of("/opt/spark/work-dir/launch_ipykernel_old.py");
                var path_eg322 = Path.of("/opt/spark/work-dir/launch_ipykernel_eg322.py");
                Files.copy(is, path);
                Files.copy(isold, path_old);
                Files.copy(iseg322, path_eg322);
                System.err.println("launch_ipykernel.py written to " + path);
            } else {
                System.err.println("launch_ipykernel.py not found");
            }
        }
    }
}
