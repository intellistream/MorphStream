package runtimeweb.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import runtimeweb.service.SignalService;

@Controller
@RequestMapping("/api/signal")
@CrossOrigin(origins = "http://localhost:4200")
public class SignalController {
    private final SignalService signalService;

    public SignalController(SignalService signalService) {
        this.signalService = signalService;
    }

    /**
     * Start a job
     *
     * @param jobId job id
     * @return True if the job is started successfully, False otherwise
     */
    @PostMapping("/start/{jobId}")
    public ResponseEntity<Boolean> onStartSignal(@PathVariable("jobId") String jobId) {
        return new ResponseEntity<>(signalService.onStartSignal(jobId), HttpStatus.OK);
    }

    /**
     * Stop a job
     *
     * @param jobId job id
     * @return True if the job is stopped successfully, False otherwise
     */
    @PostMapping("/stop/{jobId}")
    public ResponseEntity<Boolean> onStopSignal(@PathVariable("jobId") String jobId) {
        return new ResponseEntity<>(signalService.onStopSignal(jobId), HttpStatus.OK);
    }

    @PostMapping("/submit/job_config")
    public ResponseEntity<Boolean> onSubmitJobSignal(@RequestParam String jobName, @RequestParam int parallelism, @RequestParam boolean startNow,
                                                     @RequestParam String code, @RequestParam MultipartFile configFile) {
        return new ResponseEntity<>(signalService.onSubmitSignal(jobName, parallelism, startNow, code, configFile), HttpStatus.OK);
    }

    @PostMapping("/submit/job_description")
    public ResponseEntity<Boolean> onSubmitJobSignal(@RequestParam String jobName, @RequestParam int parallelism, @RequestParam boolean startNow,
                                                     @RequestParam String code, @RequestParam String description) {
        return new ResponseEntity<>(signalService.onSubmitSignal(jobName, parallelism, startNow, code, description), HttpStatus.OK);
    }

    @DeleteMapping("/delete/{jobId}")
    public ResponseEntity<Boolean> onDeleteJobSignal(@PathVariable("jobId") String jobId) {
        return new ResponseEntity<>(signalService.onDeleteSignal(jobId), HttpStatus.OK);
    }
}
