package client.jobmanage.util.initialize;

import client.jobmanage.util.Util;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class JobUploadUtil {
    public static boolean onJobUpload(MultipartFile file) throws IOException {
        if (file.isEmpty() || file.getOriginalFilename() == null || file.getOriginalFilename().isEmpty()) {
            return false;
        }
        try {
            byte[] bytes = file.getBytes();
            Util.validateAndMakeDirectory(Util.tempDirectory);
            String filePath = String.format("%s/%s", Util.tempDirectory, file.getOriginalFilename());
            Path path = Paths.get(filePath);
            Files.write(path, bytes);
            return true;
        } catch (IOException e) {
            throw new IOException("Failed to upload job: " + e.getMessage());
        }
    }

    public static boolean isUploaded(String fileName) {
        return Files.exists(Paths.get(String.format("%s/%s", Util.tempDirectory, fileName)));
    }

    public static String readFile(String fileName) {
        try {
            byte[] bytes = Files.readAllBytes(Paths.get(String.format("%s/%s", Util.tempDirectory, fileName)));
            return new String(bytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read the file: " + e.getMessage());
        }
    }

    public static void clear() {
        // delete all files in the temp directory
        if (Util.tempDirectory.exists()) {
            File[] files = Util.tempDirectory.listFiles();
            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
            }
        }
    }
}
