package client.jobmanage.util.initialize;

import client.jobmanage.util.Util;
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
 * JobInitializeUtil is used to initialize a new job
 */
public class JobInitializeUtil {
    private static final Logger log = LoggerFactory.getLogger(JobInitializeUtil.class);

    /**
     * Initialize a new job
     *
     * @param jobName the name of the job
     * @return true if the job is initialized successfully, false otherwise
     */
    public static boolean initialize(String jobName) {
        // create jobInfo directory if not exists
        if (!Util.validateDirectory(Util.jobInfoDirectory) || Util.validateDirectory(Util.jobCompileDirectory)) {
            return false;
        }

        int jobId = generateJobId();

        // create job JSON file if not exists
        String jobFileName = String.format("%s/%s.json", Util.jobInfoDirectory, jobId);
        Path jobFile = Paths.get(jobFileName);
        if (Files.exists(jobFile)) {
            log.error("Job JSON file already exists.");
            return false;
        } else {
            try {
                Files.createFile(jobFile);
            } catch (IOException e) {
                log.error("Error in creating Job JSON file: " + e.getMessage());
            }
        }

        File jobInfoFolder = new File(String.format("%s/%s", Util.jobInfoDirectory, jobId));
        Util.validateDirectory(jobInfoFolder);

        // create jobCompile directory if not exists
        File jobCompileFolder = new File(String.format("%s/%s", Util.jobCompileDirectory, jobId));
        Util.validateDirectory(jobCompileFolder);

        // create jobInfo json file for new job
        Job job = new Job(jobId, jobName);
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            objectMapper.writeValue(new File(jobFileName), job);
            log.info("Job JSON file created successfully.");
        } catch (IOException e) {
            log.error("Error in creating Job JSON file: " + e.getMessage());
        }
        return true;
    }

    /**
     * Generate a new job id
     *
     * @return job id
     */
    private static int generateJobId() {
        if (!Util.jobInfoDirectory.exists() || Util.jobInfoDirectory.listFiles() == null) {
            return 0;   // job id starts from 0
        }

        ArrayList<Integer> existingJobIds = new ArrayList<>();
        for (File file : Objects.requireNonNull(Util.jobInfoDirectory.listFiles())) {
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
