package runtimeweb.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import runtimeweb.service.SignalService;

import java.io.IOException;

@Controller
@RequestMapping("/api/signal")
@CrossOrigin(origins = "http://localhost:4200")
public class SignalController {
    private static final Logger LOG = LoggerFactory.getLogger(SignalController.class);

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

    @PostMapping("/submit/file")
    public ResponseEntity<Boolean> onSubmitFileSignal(@RequestParam("file") MultipartFile file) {
        String fileName = file.getOriginalFilename();
        if (fileName == null) {
            LOG.error("Failed to get the file name");
            return new ResponseEntity<>(false, HttpStatus.BAD_REQUEST);
        }
        try {
            boolean submitResult = signalService.onSubmitFileSignal(file);
            if (!submitResult) {
                LOG.error("Failed to submit the file");
                return new ResponseEntity<>(false, HttpStatus.BAD_REQUEST);
            }
        } catch (IOException e) {
            LOG.error("Failed to submit the file: " + e.getMessage());
            return new ResponseEntity<>(false, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>(true, HttpStatus.OK);
    }

    @PostMapping("/submit/confirm_files")
    public ResponseEntity<Boolean> onConfirmFilesSubmitSignal(@RequestParam("files") String[] fileNames, @RequestParam("parallelism") int parallelism, @RequestParam("startNow") boolean startNow) {
        if (signalService.onConfirmFilesSubmitSignal(fileNames, parallelism, startNow)) {
            return new ResponseEntity<>(true, HttpStatus.OK);
        } else {
            return new ResponseEntity<>(false, HttpStatus.BAD_REQUEST);
        }
    }
}
