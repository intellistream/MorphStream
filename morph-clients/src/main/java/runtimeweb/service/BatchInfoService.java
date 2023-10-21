package runtimeweb.service;

import communication.dao.Batch;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class BatchInfoService extends AbstractService {
    /**
     * Get all batches of a job
     * @return a list of batches
     */
    public List<Batch> findAllBatches(String jobId, String operatorId) {
        File directory = new File(PATH+"\\"+jobId+"\\"+operatorId);
        List<Batch> batches = new ArrayList<>();
        if (directory.exists() && directory.isDirectory()) {
            FilenameFilter jsonFilter = (dir, name) -> name.endsWith(".json");
            File[] jsonFiles = directory.listFiles(jsonFilter);
            if (jsonFiles != null) {
                for (File jsonFile: jsonFiles) {
                    Batch batch = null;
                    try {
                        batch = objectMapper.readValue(jsonFile, Batch.class);
                        batches.add(batch);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return batches;
    }

    /**
     * Get a batch by operator id and batch id
     * @param operatorId operator id
     * @param batchId  batch id
     * @return a batch
     */
   public Batch findBatchById(String jobId, String operatorId, String batchId) {
        File directory = new File(PATH+"\\"+jobId+"\\"+operatorId);
        if (directory.exists() && directory.isDirectory()) {
            System.out.println(batchId + ".json");
            FilenameFilter jsonFilter = (dir, name) -> name.equals(batchId + ".json");
            File[] jsonFiles = directory.listFiles(jsonFilter);

            if (jsonFiles != null && jsonFiles.length == 1) {
                try {
                    return objectMapper.readValue(jsonFiles[0], Batch.class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }
}
