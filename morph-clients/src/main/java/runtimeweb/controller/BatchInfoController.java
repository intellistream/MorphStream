package runtimeweb.controller;

import dao.Batch;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import runtimeweb.service.BatchInfoService;

import java.util.List;

@Controller
@RequestMapping("/batchInfo")
@CrossOrigin
public class BatchInfoController {
    private final BatchInfoService batchInfoService;

    public BatchInfoController(BatchInfoService batchInfoService) {
        this.batchInfoService = batchInfoService;
    }

    @GetMapping("/get/all/{jobId}/{operatorId}")
    public ResponseEntity<List<Batch>> getAllBatches(@PathVariable("jobId") String jobId, @PathVariable("operatorId") String operatorId) {
        List<Batch> batches = batchInfoService.findAllBatches(jobId, operatorId);
        return new ResponseEntity<>(batches, org.springframework.http.HttpStatus.OK);
    }

    @GetMapping("/get/{jobId}/{batchId}/{operatorId}")
    public ResponseEntity<Batch> getBatchById(@PathVariable("jobId") String jobId, @PathVariable("operatorId") String operatorId, @PathVariable("batchId") String batchId) {
        Batch batch = batchInfoService.findBatchById(jobId, operatorId, batchId);
        return new ResponseEntity<>(batch, org.springframework.http.HttpStatus.OK);
    }
}
