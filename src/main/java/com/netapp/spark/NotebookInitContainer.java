package com.netapp.spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class NotebookInitContainer {
    public static void main(String[] args) throws IOException {
        var sparkDir = Path.of("/opt/spark");
        var workDir = sparkDir.resolve("work-dir");
        var libDir = workDir.resolve("lib");
        var jarsDir = sparkDir.resolve("jars");
        try (var is = NotebookInitContainer.class.getResourceAsStream("/launch_ipykernel.py");
             var isold = NotebookInitContainer.class.getResourceAsStream("/launch_ipykernel-v2.3.1.py");
             var iseg322 = NotebookInitContainer.class.getResourceAsStream("/launch_ipykernel-v3.2.2.py")) {
            if (is != null && isold != null && iseg322 != null) {
                Files.createDirectories(libDir);
                var path = workDir.resolve("launch_ipykernel.py");
                var path_old = workDir.resolve("launch_ipykernel-v2.3.1.py");
                var path_eg322 = workDir.resolve("launch_ipykernel-v3.2.2.py");
                Files.copy(is, path);
                Files.copy(isold, path_old);
                Files.copy(iseg322, path_eg322);
                var toreePath = jarsDir.resolve("toree.jar");
                if (Files.exists(toreePath)) Files.copy(toreePath, libDir.resolve("toree.jar"));
                System.err.println("launch_ipykernel.py written to " + path);
            } else {
                System.err.println("launch_ipykernel.py not found");
            }
        }
    }
}
