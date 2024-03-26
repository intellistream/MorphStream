package client.jobmanage.util;

import client.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class Util {
    private static final Logger log = LoggerFactory.getLogger(Util.class);

    public static final File jobInfoDirectory = new File(String.format("%s", Configuration.JOB_INFO_PATH));

    public static final File jobCompileDirectory = new File(String.format("%s", Configuration.JOB_COMPILE_PATH));

    public static final File compileDependencyDirectory = new File(String.format("%s", Configuration.COMPILE_DEPENDENCY_PATH));

    /**
     * Util method that check the validity of a directory
     * @param directory the directory to be validated
     * @return true if the directory is valid, false otherwise
     */
    public static boolean validateAndMakeDirectory(File directory) {
        if (directory.exists() && directory.isDirectory()) {
            return true;
        } else {
            if (directory.mkdirs()) {
                log.info("Directory is created!");
                return true;
            } else {
                log.info("Failed to create directory!");
                return false;
            }
        }
    }

    /**
     * Util method that check the validity of a file
     * @param directory the file to be validated
     * @return true if the file is valid, false otherwise
     */
    public static boolean validateFile(File directory) {
        return directory.exists() && directory.isFile();
    }
}
