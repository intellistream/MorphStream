package common.io.LocalFS;

import java.io.File;
import java.util.Objects;

public class FileSystem {
    public static Boolean deleteFile(File file) {
        if (file == null || !file.exists()) {
            return false;
        }
        for (File f : Objects.requireNonNull(file.listFiles())) {
            if (f.isDirectory()) {
                deleteFile(f);
            } else {
                f.delete();
            }
        }
        file.delete();
        return true;
    }

    /**
     *
     * @param file
     * @return File Size
     * @throws Exception
     */
    public static long getFileSize(File file) throws Exception {
        long size = 0;
        if (file.isFile()) {
            if (file.exists()) {
                size = file.length();
            }
        } else {
            final File[] children = file.listFiles();
            if (children != null && children.length > 0) {
                for (final File child : children) {
                    size += getFileSize(child);
                }
            }

        }
        return size;
    }

}
