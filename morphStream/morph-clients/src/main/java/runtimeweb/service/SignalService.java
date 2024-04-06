package runtimeweb.service;

import cli.SLClient;
import cli.WebServer;
import intellistream.morphstream.api.input.TPGInputListener;
import org.springframework.stereotype.Service;

@Service
public class SignalService extends AbstractService {
    /**
     * Start a job
     * @param jobId job id
     * @return true if the job is started successfully, false otherwise
     */
    public Boolean onStartSignal(String jobId) {
        WebServer.createJobInfoJSON("StreamLedger");
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
        TPGInputListener.get().insertStopSignal(); // notify spout to pass stop signal downstream
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
        WebServer.createJobInfoJSON(jobName);
        if (startNow) {
            try {
                SLClient.startJob(new String[]{}); // start the job
                return true;
            } catch (Exception e) {
                return false;
            }
        } else {
            return true;
        }
    }
}
