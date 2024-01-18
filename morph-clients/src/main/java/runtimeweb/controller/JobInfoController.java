package runtimeweb.controller;

import dao.Job;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import runtimeweb.service.JobInfoService;

import java.util.List;

@Controller
@RequestMapping("/jobInfo")
@CrossOrigin
public class JobInfoController {
    private final JobInfoService jobInfoService;

    public JobInfoController(JobInfoService jobInfoService) {
        this.jobInfoService = jobInfoService;
    }

    @GetMapping("/get/all")
    public ResponseEntity<List<Job>> getAllJobInfos() {
        List<Job> jobInfos = jobInfoService.findAllJobInfos();
        return new ResponseEntity<>(jobInfos, HttpStatus.OK);
    }

    @GetMapping("/get/{jobId}")
    public ResponseEntity<Job> getJobById(@PathVariable("jobId") Integer jobId) {
        Job jobInfo = jobInfoService.findJobById(String.valueOf(jobId));
        return new ResponseEntity<>(jobInfo, HttpStatus.OK);
    }
}
