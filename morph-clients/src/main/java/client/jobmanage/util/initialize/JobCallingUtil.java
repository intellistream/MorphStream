package client.jobmanage.util.initialize;

import client.jobmanage.util.Util;
import client.jobmanage.util.seek.JobSeekUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;

/**
 * JobCallingUtil is used to compile, start a job
 */
public class JobCallingUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JobCallingUtil.class);

    /**
     * Compile a job by job id
     *
     * @param jobId job id
     */
    public static void compileJobById(int jobId) {
        String jobName = JobSeekUtil.getJobNameById(jobId);
        String jobCodeFolder = String.format("%s/%s", Util.jobCompileDirectory, jobId);
        String jobCodePath = String.format("%s/%s.java", jobCodeFolder, jobName);
        File jobCode = new File(jobCodePath);

        if (!Util.validateFile(jobCode)) {
            // job code does not exist
            LOG.error("Job code does not exist");
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
     *
     * @param jobName job name
     */
    public static void compileJobByName(String jobName) {
        Integer jobId = JobSeekUtil.getJobIdByName(jobName);
        if (jobId == null) {
            // job does not exist
            LOG.error("Job does not exist");
            return;
        }
        compileJobById(jobId);
    }


    /**
     * Start a job by job id
     *
     * @param jobId job id
     */
    public static void startJobById(int jobId) {
        String jobName = JobSeekUtil.getJobNameById(jobId);
        String jobCodeFolder = String.format("%s/%s", Util.jobCompileDirectory, jobId);
        String jobClassPath = String.format("%s/%s.class", jobCodeFolder, jobName);

        if (!Util.validateFile(new File(jobClassPath))) {
            // job class does not exist
            LOG.error("Job class does not exist");
            return;
        }

        String classpathSeparator = System.getProperty("os.name").toLowerCase().contains("win") ? ";" : ":";

        // start the job
        try {
            LOG.info("Starting job: " + jobName);
            ProcessBuilder builder = new ProcessBuilder(
                    "java", "-classpath",
                    jobCodeFolder + classpathSeparator +
                            Util.compileDependencyDirectory + "/morph-clients.jar" + classpathSeparator +
                            Util.compileDependencyDirectory + "/morph-common.jar" + classpathSeparator +
                            Util.compileDependencyDirectory + "/morph-core.jar",
                    jobName
            );
            builder.redirectErrorStream(true);
            Process process = builder.start();

            BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                LOG.info(line);
            }
            process.waitFor();  // wait for the process to finish
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to start job: " + e.getMessage());
        }
    }

    /**
     * Start a job by job name
     *
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
