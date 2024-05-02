package runtimeweb.service;

import client.impl.SLClient;
import client.jobmanage.util.initialize.JobCallingUtil;
import client.jobmanage.util.initialize.JobPrepareUtil;
import client.jobmanage.util.seek.JobSeekUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dao.config.JobConfiguration;
import intellistream.morphstream.api.input.InputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import client.jobmanage.util.initialize.JobInitializeUtil;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@Service
public class SignalService {
    private static final Logger LOG = LoggerFactory.getLogger(SignalService.class);

    /**
     * Start a job
     * @param jobId job id
     * @return true if the job is started successfully, false otherwise
     */
    public Boolean onStartSignal(String jobId) {
//        JobInitializeUtil.initialize(jobId); // initialize the job
        JobPrepareUtil.prepare(jobId); // prepare the job
        // TODO: start job based on job id
        try {
            SLClient.startJob(new String[]{}); // start the job
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * Stop a job
     * @param jobId job id
     * @return true if the job is stopped successfully, false otherwise
     */
    public Boolean onStopSignal(String jobId) {
        InputSource.get().insertStopSignal(); // notify spout to pass stop signal downstream
        return true;
    }

    public Boolean onSubmitSignal(String jobName, int parallelism, boolean startNow, String code, String description) {
        JobConfiguration jobConfiguration;
        LOG.info("Job submitted: " + jobName);

        try {
            jobConfiguration = new ObjectMapper().readValue(description, JobConfiguration.class);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to parse the job configuration");
            return false;
        }

        boolean initialized = JobInitializeUtil.initialize(jobName, parallelism, jobConfiguration); // initialize the job
        if (!initialized) {
            LOG.error("Failed to initialize the job");
            return false;
        }

        code = JobInitializeUtil.preprocessedCode(code, jobConfiguration);
        JobInitializeUtil.saveCode(code, String.valueOf(JobSeekUtil.getJobIdByName(jobName)), jobName);

        if (startNow) {
            try {
                JobCallingUtil.compileJobByName(jobName);
                JobCallingUtil.startJobByName(jobName);
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    /**
     * Submit a job
     * @param jobName job name
     * @param parallelism parallelism
     * @param startNow start now
     * @param code code
     * @return true if the job is submitted successfully, false otherwise
     */
    public Boolean onSubmitSignal(String jobName, int parallelism, boolean startNow, String code, MultipartFile configFile) {
        // save the config file
        LOG.info("File uploaded: " + configFile.getOriginalFilename());

        try {
            String description = new String(configFile.getBytes());
            onSubmitSignal(jobName, parallelism, startNow, code, description);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read the config file");
        }
        return true;
    }

    public Boolean onDeleteSignal(String jobId) {
        JobSeekUtil.removeJob(jobId);
        return true;
    }
}
