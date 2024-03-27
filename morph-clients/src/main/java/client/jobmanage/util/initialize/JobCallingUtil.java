package client.jobmanage.util.initialize;

import client.jobmanage.util.Util;
import client.jobmanage.util.seek.JobSeekUtil;

import java.io.File;

/**
 * JobCallingUtil is used to compile, start a job
 */
public class JobCallingUtil {
    /**
     * Compile a job by job id
     * @param jobId job id
     */
    public static void compileJobById(int jobId) {
        String jobName = JobSeekUtil.getJobNameById(jobId);
        String jobCodeFolder = String.format("%s/%s", Util.jobCompileDirectory, jobId);
        String jobCodePath = String.format("%s/%s.java", jobCodeFolder, jobName);
        File jobCode = new File(jobCodePath);

        if (!Util.validateFile(jobCode)) {
            // job code does not exist
            return;
        }

        String compileCommand = String.format("javac -classpath \"%s/morph-clients.jar;%s/morph-common.jar;%s/morph-core.jar\" -d \"%s\" \"%s\"",
                Util.compileDependencyDirectory, Util.compileDependencyDirectory, Util.compileDependencyDirectory, jobCodeFolder, jobCodePath);

        // compile the job
        try {
            Process process = Runtime.getRuntime().exec(compileCommand);
            process.waitFor();
        } catch (Exception e) {
            throw new RuntimeException("Failed to compile job: " + e.getMessage());
        }
    }

    /**
     * Compile a job by job name
     * @param jobName job name
     */
    public static void compileJobByName(String jobName) {
        Integer jobId = JobSeekUtil.getJobIdByName(jobName);
        if (jobId == null) {
            // job does not exist
            return;
        }
        compileJobById(jobId);
    }


    /**
     * Start a job by job id
     * @param jobId job id
     */
    public static void startJobById(int jobId) {
        String jobName = JobSeekUtil.getJobNameById(jobId);
        String jobCodeFolder = String.format("%s/%s", Util.jobCompileDirectory, jobId);
        String jobClassPath = String.format("%s/%s.class", jobCodeFolder, jobName);

        if (!Util.validateFile(new File(jobClassPath))) {
            // job class does not exist
            return;
        }

        String runningCommand = String.format("java -classpath \"%s;%s/morph-clients.jar;%s/morph-common.jar;%s/morph-core.jar\" %s",
                jobCodeFolder,
                Util.compileDependencyDirectory,
                Util.compileDependencyDirectory,
                Util.compileDependencyDirectory,
                jobName
        );

        // start the job
        try {
            Process process = Runtime.getRuntime().exec(runningCommand);
             process.waitFor();  // wait for the process to finish
        } catch (Exception e) {
            throw new RuntimeException("Failed to start job: " + e.getMessage());
        }
    }

    /**
     * Start a job by job name
     * @param jobName job name
     */
    public static void startJobByName(String jobName) {
        Integer jobId = JobSeekUtil.getJobIdByName(jobName);
        if (jobId == null) {
            // job does not exist
            return;
        }
        startJobById(jobId);
    }
}
