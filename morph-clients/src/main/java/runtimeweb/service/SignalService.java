package runtimeweb.service;

import cli.SLClient;
import cli.WebServer;
import intellistream.morphstream.api.input.InputSource;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
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
        InputSource.get().insertStopSignal(MorphStreamEnv.get().configuration().getInt("spoutNum")); // notify spout to pass stop signal downstream
        return true;
    }
}
