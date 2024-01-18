package runtimeweb.service;

import dao.Job;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class JobInfoService extends AbstractService {
    /**
     * Get all job infos
     *
     * @return a list of job infos
     */
    public List<Job> findAllJobInfos() {
        File directory = new File(PATH);
        List<Job> jobs = new ArrayList<>();
        if (directory.exists() && directory.isDirectory()) {
            // read all json files in the directory
            FilenameFilter jsonFilter = (dir, name) -> name.endsWith(".json");
            File[] jsonFiles = directory.listFiles(jsonFilter);
            if (jsonFiles != null) {
                for (File jsonFile : jsonFiles) {
                    Job job;
                    try {
                        job = objectMapper.readValue(jsonFile, Job.class);
                        jobs.add(job);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return jobs;
    }

    /**
     * Get a job info by job id
     *
     * @param jobId job id
     * @return a job info
     */
    public Job findJobById(String jobId) {
        File directory = new File(PATH);
        if (directory.exists() && directory.isDirectory()) {
            // read all json files in the directory
            FilenameFilter jsonFilter = (dir, name) -> name.equals(jobId + ".json");
            File[] jsonFiles = directory.listFiles(jsonFilter);
            if (jsonFiles != null && jsonFiles.length == 1) {
                try {
                    return objectMapper.readValue(jsonFiles[0], Job.class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }
}
