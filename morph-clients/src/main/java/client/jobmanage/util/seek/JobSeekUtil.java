package client.jobmanage.util.seek;

import client.jobmanage.util.Util;
import com.fasterxml.jackson.databind.ObjectMapper;
import dao.Batch;
import dao.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * JobInfoSeekUtil contains utils used to seek job info from local file system
 */
public class JobSeekUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(JobSeekUtil.class);

    /**
     * Get all jobs
     * @return a list of jobs
     */
    public static List<Job> getAllJobs() {
        List<Job> jobs = new ArrayList<>();

        if (Util.validateAndMakeDirectory(Util.jobInfoDirectory)) {
            // read all json files in the directory
            FilenameFilter jsonFilter = (dir, name) -> name.endsWith(".json");
            File[] jsonFiles = Util.jobInfoDirectory.listFiles(jsonFilter);
            if (jsonFiles != null) {
                for (File jsonFile : jsonFiles) {
                    Job job;
                    try {
                        job = objectMapper.readValue(jsonFile, Job.class);
                        jobs.add(job);
                    } catch (IOException e) {
                        log.info("Failed to read job info from file: " + jsonFile.getName());
                    }
                }
            }
        }
        return jobs;
    }

    /**
     * Get a job by job id
     * @param jobId job id
     * @return a job object
     */
    public static Job getJobById(String jobId) {
        if (Util.validateAndMakeDirectory(Util.jobInfoDirectory)) {
            FilenameFilter jsonFilter = (dir, name) -> name.equals(jobId + ".json");
            File[] jsonFiles = Util.jobInfoDirectory.listFiles(jsonFilter);
            if (jsonFiles != null && jsonFiles.length == 1) {
                try {
                    // read the json file into a Job object
                    return objectMapper.readValue(jsonFiles[0], Job.class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }

    /**
     * Get all batches of a job
     * @param jobId the id of the job
     * @param operatorId the id of the operator
     * @return a list of batches
     */
    public static List<Batch> getAllBatches(String jobId, String operatorId) {
        List<Batch> batches = new ArrayList<>();
        File directory = new File(String.format("%s/%s/%s", Util.jobInfoDirectory, jobId, operatorId));
        if (Util.validateAndMakeDirectory(directory)) {
            FilenameFilter jsonFilter = (dir, name) -> name.endsWith(".json");
            File[] jsonFiles = directory.listFiles(jsonFilter);
            if (jsonFiles != null) {
                for (File jsonFile: jsonFiles) {
                    Batch batch;
                    try {
                        batch = objectMapper.readValue(jsonFile, Batch.class);
                        batches.add(batch);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return batches;
    }

    /**
     * Get a batch by operator id and batch id
     * @param jobId the id of the job
     * @param operatorId the id of the operator
     * @param batchId the id of the batch
     * @return a batch
     */
    public static Batch getBatchById(String jobId, String operatorId, String batchId) {
        File directory = new File(String.format("%s/%s/%s", Util.jobInfoDirectory, jobId, operatorId));
        if (Util.validateAndMakeDirectory(directory)) {
            FilenameFilter jsonFilter = (dir, name) -> name.equals(batchId + ".json");
            File[] jsonFiles = directory.listFiles(jsonFilter);

            if (jsonFiles != null && jsonFiles.length == 1) {
                try {
                    return objectMapper.readValue(jsonFiles[0], Batch.class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }

    /**
     * Get the job id by job name
     * @param jobName the name of the job
     * @return the id of the job
     */
    public static Integer getJobIdByName(String jobName) {
        if (Util.validateAndMakeDirectory(Util.jobInfoDirectory)) {
            // read all json files and check if the job name matches
            List<Job> jobs = getAllJobs();
            for (Job job : jobs) {
                if (job.getName().equals(jobName)) {
                    return job.getJobId();
                }
            }
        }
        return null;
    }

    /**
     * Get the job name by job id
     * @param jobId the id of the job
     * @return the name of the job
     */
    public static String getJobNameById(int jobId) {
        if (Util.validateAndMakeDirectory(Util.jobInfoDirectory)) {
            // read all json files and check if the job name matches
            List<Job> jobs = getAllJobs();
            for (Job job : jobs) {
                if (job.getJobId() == jobId) {
                    return job.getName();
                }
            }
        }
        return null;
    }
}
