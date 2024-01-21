package runtimeweb.service;

import client.jobmanage.JobManager;
import dao.Batch;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class BatchInfoService {
    /**
     * Get all batches of a job
     * @return a list of batches
     */
    public List<Batch> findAllBatches(String jobId, String operatorId) {
        return JobManager.getInstance().getAllBatches(jobId, operatorId);
    }

    /**
     * Get a batch by operator id and batch id
     * @param operatorId operator id
     * @param batchId  batch id
     * @return a batch
     */
   public Batch findBatchById(String jobId, String operatorId, String batchId) {
        return JobManager.getInstance().getBatchById(jobId, operatorId, batchId);
    }
}
