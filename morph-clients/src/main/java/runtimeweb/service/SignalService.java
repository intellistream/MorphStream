package runtimeweb.service;

import client.impl.SLClient;
import intellistream.morphstream.api.input.InputSource;
import org.springframework.stereotype.Service;
import client.jobmanage.util.initialize.JobInitializeUtil;

@Service
public class SignalService {
    /**
     * Start a job
     * @param jobId job id
     * @return true if the job is started successfully, false otherwise
     */
    public Boolean onStartSignal(String jobId) {
        JobInitializeUtil.initialize(jobId); // initialize the job
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
    public Boolean onSubmitSignal(String jobName, int parallelism, boolean startNow, String code) {
//        System.out.println(code);
        // TODO: analyze code and generate job
        boolean initialized = JobInitializeUtil.initialize(jobName); // initialize the job
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
