package client.jobmanage.util.initialize;

import client.jobmanage.util.Util;
import com.fasterxml.jackson.databind.ObjectMapper;
import dao.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class JobPrepareUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(JobPrepareUtil.class);

    /**
     * Prepare a job for execution
     *
     * @param jobId job id
     * @return the prepared job
     */
    public static Job prepare(String jobId) {
        // create jobInfo directory if not exists
        if (!Util.validateAndMakeDirectory(Util.jobInfoDirectory) || Util.validateAndMakeDirectory(Util.jobCompileDirectory)) {
            return null;
        }

        String jobFileName = String.format("%s/%s.json", Util.jobInfoDirectory, jobId);
        File jobFile = new File(jobFileName);
        if (!Files.exists(jobFile.toPath())) {
            log.error("Job JSON file does not exist.");
            return null;
        } else {
            // read job JSON file into Job object
            try {
                Job job = objectMapper.readValue(jobFile, Job.class);

                // create jobInfo directory and operatorInfo directory if not exists
                File jobInfoFolder = new File(String.format("%s/%s", Util.jobInfoDirectory, jobId));
                Util.validateAndMakeDirectory(jobInfoFolder);

                job.getOperators().forEach(operator -> {
                    File operatorInfoFolder = new File(String.format("%s/%s/%s", Util.jobInfoDirectory, jobId, operator.getId()));
                    Util.validateAndMakeDirectory(operatorInfoFolder);
                });
                return job;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
