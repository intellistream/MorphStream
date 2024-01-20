package client.jobmanage.initializer;

import client.Configuration;
import com.fasterxml.jackson.databind.ObjectMapper;
import dao.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Objects;

/**
 * JobInitializer is used to initialize a new job
 */
public class JobInitializer {
    private static final Logger log = LoggerFactory.getLogger(JobInitializer.class);

    /**
     * Initialize a new job
     *
     * @param jobName the name of the job
     * @return true if the job is initialized successfully, false otherwise
     */
    public static boolean initialize(String jobName) {
        // create jobInfo directory if not exists
        File directory = new File(String.format("%s", Configuration.JOB_INFO_PATH));
        if (!directory.exists()) {
            if (directory.mkdirs()) {
                log.info("Directory is created!");
            } else {
                log.info("Failed to create directory!");
                return false;
            }
        }

        int jobId = generateJobId();
        jobName = jobName.trim().replace(" ", "_");

        // create job JSON file if not exists
        String jobFileName = String.format("%s/%s.json", directory, jobId + "_" + jobName);
        Path jobFile = Paths.get(jobFileName);
        if (Files.exists(jobFile)) {
            log.error("Job JSON file already exists.");
            return false;
        } else {
            try {
                Files.createFile(jobFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // create jobCompile directory if not exists
        File jobInfoDirectory = new File(String.format("%s/%s", Configuration.JOB_INFO_PATH, jobId + "_" + jobName));
        if (!jobInfoDirectory.exists()) {
            if (jobInfoDirectory.mkdirs()) {
                log.info("Directory is created!");
            } else {
                log.info("Failed to create directory!");
                return false;
            }
        }

        // create jobCompile directory if not exists
        File jobCompileDirectory = new File(String.format("%s/%s", Configuration.JOB_COMPILE_PATH, jobId + "_" + jobName));
        if (!jobCompileDirectory.exists()) {
            if (jobCompileDirectory.mkdirs()) {
                log.info("Directory is created!");
            } else {
                log.info("Failed to create directory!");
                return false;
            }
        }

        // create jobInfo json file for new job
        Job job = new Job(jobId, jobName);
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            objectMapper.writeValue(new File(jobFileName), job);
            log.info("Job JSON file created successfully.");
        } catch (IOException e) {
            throw new RuntimeException("Error in creating Job JSON file: " + e.getMessage());
        }
        return true;
    }

    /**
     * Generate a new job id
     *
     * @return job id
     */
    private static int generateJobId() {
        File jobInfoDirectory = new File(Configuration.JOB_INFO_PATH);
        if (!jobInfoDirectory.exists() || jobInfoDirectory.listFiles() == null) {
            return 0;   // job id starts from 0
        }

        ArrayList<Integer> existingJobIds = new ArrayList<>();
        for (File file : Objects.requireNonNull(jobInfoDirectory.listFiles())) {
            String fileName = file.getName();
            if (fileName.endsWith(".json")) {
                fileName = fileName.replace(".json", "");
                try {
                    int fileNumber = Integer.parseInt(fileName);
                    existingJobIds.add(fileNumber);
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }

        // find the smallest positive integer that is not in the list
        int jobId = 0;
        while (existingJobIds.contains(jobId)) {
            jobId++;
        }
        return jobId;
    }
}
