package runtimeweb.service;

import client.jobmanage.JobManager;
import dao.Job;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class JobInfoService {
    /**
     * Get all job infos
     *
     * @return a list of job infos
     */
    public List<Job> findAllJobInfos() {
        return JobManager.getInstance().getAllJobs();
    }

    /**
     * Get a job info by job id
     *
     * @param jobId job id
     * @return a job info
     */
    public Job findJobById(String jobId) {
        return JobManager.getInstance().getJobById(jobId);
    }
}
