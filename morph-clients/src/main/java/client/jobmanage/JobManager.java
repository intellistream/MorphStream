package client.jobmanage;

import client.jobmanage.util.seek.JobSeekUtil;
import dao.Batch;
import dao.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * JobManager manages the jobs that are submitted to Morph dashboard
 */
public class JobManager {
    private static final JobManager jobManager = new JobManager();
    private final Logger log = LoggerFactory.getLogger(JobManager.class);

    private JobManager() {}

    public static JobManager getInstance() {
        return JobManager.jobManager;
    }

    /**
     * Get all jobs
     * @return a list of jobs
     */
    public List<Job> getAllJobs() {
        return JobSeekUtil.getAllJobs();
    }

    /**
     * Get a job by job id
     * @param jobId the id of the job
     * @return the job
     */
    public Job getJobById(String jobId) {
        return JobSeekUtil.getJobById(jobId);
    }

    /**
     * Get all batches of a job
     * @return a list of batches
     */
    public List<Batch> getAllBatches(String jobId, String operatorId) {
        return JobSeekUtil.getAllBatches(jobId, operatorId);
    }

    /**
     * Get a batch by operator id and batch id
     * @param operatorId operator id
     * @param batchId  batch id
     * @return a batch
     */
    public Batch getBatchById(String jobId, String operatorId, String batchId) {
        return JobSeekUtil.getBatchById(jobId, operatorId, batchId);
    }

    public void removeJob(String jobId) {
        JobSeekUtil.removeJob(jobId);
    }
}
