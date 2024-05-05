package runtimeweb.service;

import client.jobmanage.util.initialize.JobCallingUtil;
import client.jobmanage.util.initialize.JobPrepareUtil;
import client.jobmanage.util.initialize.JobUploadUtil;
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
        LOG.info("Job started: " + jobId);
        try {
            JobCallingUtil.compileJobById(Integer.parseInt(jobId)); // compile the job
            JobCallingUtil.startJobById(Integer.parseInt(jobId)); // start the job
//            SLClient.startJob(new String[]{}); // start the job
        } catch (Exception e) {
            e.printStackTrace();
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

        Integer jobId = JobInitializeUtil.initialize(jobName, parallelism, jobConfiguration); // initialize the job
        if (jobId == null) {
            LOG.error("Failed to initialize the job");
            return false;
        }

        code = JobInitializeUtil.preprocessedCode(code, String.valueOf(jobId), jobConfiguration);
        JobInitializeUtil.saveCode(code, String.valueOf(jobId), jobName);

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

    public Boolean onSubmitFileSignal(MultipartFile file) throws IOException {
        String fileName = file.getOriginalFilename();
        if (fileName == null) {
            LOG.error("Failed to get the file name");
            return false;
        }
        return JobUploadUtil.onJobUpload(file);
    }
    public Boolean onConfirmFilesSubmitSignal(String[] fileNames, int parallelism, boolean startNow) {
        if (fileNames == null || fileNames.length != 2) {
            return false;
        }

        for (String fileName : fileNames) {
            // check if the files are uploaded
            if (!JobUploadUtil.isUploaded(fileName)) {
                return false;
            }
        }

        String jobName = "";
        String code = "";
        String description = "";
        // read the files (.java and .json)
        for (String fileName : fileNames) {
            if (fileName.endsWith(".java")) {
                code = JobUploadUtil.readFile(fileName);
            } else if (fileName.endsWith(".json")) {
                description = JobUploadUtil.readFile(fileName);
            }
        }
        jobName = fileNames[0].substring(0, fileNames[0].length() - 5);
        JobUploadUtil.clear();
        return onSubmitSignal(jobName, parallelism, startNow, code, description);
    }
}
