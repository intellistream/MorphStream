package taskmanager.initializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dao.Job;
import dao.Operator;
import dao.OverallTimeBreakdown;
import dao.SchedulerTimeBreakdown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class JobInitializer {
    private static final Logger log = LoggerFactory.getLogger(JobInitializer.class);

    private static final String dataPath = "data/jobs"; // the path to store job info

    public static void initialize(String newJobId) {
        File directory = new File(String.format("%s", dataPath));
        if (!directory.exists()) {
            if (directory.mkdirs()) {
                System.out.println("Directory created successfully.");
            } else {
                System.out.println("Failed to create directory.");
                return;
            }
        }

        String newJobInfoFile = String.format("%s/%s.json", directory, newJobId);
        Path inputFile = Paths.get(newJobInfoFile);

        // create jobInfo json file for the new job
        try {
            if (!Files.exists(inputFile) || !Files.exists(inputFile.getParent())) {
                Files.createDirectories(inputFile.getParent());
                Files.createFile(inputFile);
            }
        } catch (IOException e) {
            System.out.println("Error in locating input file: " + e.getMessage());
        }

        // create jobInfo json file for new job
        LocalDateTime localDateTime = LocalDateTime.now();
        String jobStartTime = String.format("%s-%s-%s %s:%s:%s",
                localDateTime.getYear(), localDateTime.getMonthValue(), localDateTime.getDayOfMonth(),
                localDateTime.getHour(), localDateTime.getMinute(), localDateTime.getSecond());

        Operator operator1 = new Operator(
                "1", "SL", 4, -1, -1, "NA", "NA", "NA");
        List<Operator> operators = new ArrayList<>();
        operators.add(operator1);

        OverallTimeBreakdown overallTimeBreakdown = new OverallTimeBreakdown(0, 0, 0, 0);
        SchedulerTimeBreakdown schedulerTimeBreakdown = new SchedulerTimeBreakdown(0, 0, 0, 0, 0);

        Job application = new Job(
                3, "Stream Ledger", 4, "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz", "16GB",
                jobStartTime, "0", false, 2500, 0, 0, 0,
                0, 0, 8, new ArrayList<>(), new ArrayList<>(),
                operators, overallTimeBreakdown, schedulerTimeBreakdown);
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            objectMapper.writeValue(new File(newJobInfoFile), application);
            log.info("JSON file created successfully.");
        } catch (IOException e) {
            throw new RuntimeException("Error in creating JSON file: " + e.getMessage());
        }
    }
}
