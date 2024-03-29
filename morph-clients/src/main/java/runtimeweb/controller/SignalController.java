package runtimeweb.controller;

import com.sun.org.apache.xpath.internal.operations.Bool;
import communication.dao.JobSubmit;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import runtimeweb.service.SignalService;

@Controller
@RequestMapping("/api/signal")
@CrossOrigin
public class SignalController {
    private final SignalService signalService;

    public SignalController(SignalService signalService) {
        this.signalService = signalService;
    }

    /**
     * Start a job
     * @param jobId job id
     * @return True if the job is started successfully, False otherwise
     */
    @PostMapping("/start/{jobId}")
    public ResponseEntity<Boolean> onStartSignal(String jobId) {
        return new ResponseEntity<>(signalService.onStartSignal(jobId), HttpStatus.OK);
    }

    /**
     * Stop a job
     * @param jobId job id
     * @return True if the job is stopped successfully, False otherwise
     */
    @PostMapping("/stop/{jobId}")
    public ResponseEntity<Boolean> onStopSignal(String jobId) {
        return new ResponseEntity<>(signalService.onStopSignal(jobId), HttpStatus.OK);
    }

    @PostMapping("/submit")
    public ResponseEntity<Boolean> onSubmitSignal(@RequestBody JobSubmit job) {
        return new ResponseEntity<>(signalService.onSubmitSignal(job.getJob(), job.getParallelism(), job.isStartNow(), job.getCode()), HttpStatus.OK);
    }
}
