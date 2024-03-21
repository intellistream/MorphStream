package runtimeweb.service;

import client.impl.SLClient;
import client.jobmanage.util.initialize.JobPrepareUtil;
import intellistream.morphstream.api.input.InputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import client.jobmanage.util.initialize.JobInitializeUtil;
import org.springframework.web.multipart.MultipartFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

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
        try {
            LOG.info("File uploaded: " + configFile.getOriginalFilename());
            String fileName = configFile.getOriginalFilename();
            Path path = Paths.get("./" + fileName); // TODO: Change the path to the correct location
            Files.copy(configFile.getInputStream(), path, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            LOG.error("Failed to upload the file", e);
        }
        // TODO: analyze code and generate job

        boolean initialized = JobInitializeUtil.initialize(jobName, parallelism); // initialize the job
        if (!initialized) {
            return false;
        }
        if (startNow) {
            try {
                SLClient.startJob(new String[]{}); // start the job
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }
}
