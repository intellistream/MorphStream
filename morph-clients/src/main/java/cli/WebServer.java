package cli;

import com.esotericsoftware.minlog.Log;
import com.fasterxml.jackson.databind.ObjectMapper;
import runtimeweb.handler.WebSocketHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class WebServer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(WebServer.class);
    private static final EventLoopGroup bossGroup = new NioEventLoopGroup(); //for message transmission over websocket
    private static final EventLoopGroup workerGroup = new NioEventLoopGroup(2);
    private static final WebSocketHandler webSocketHandler = new WebSocketHandler();
//    private static final String jobInfoDirectory = "morph-clients/src/main/java/cli/jobInfo";
    private static final String dataPath = "data/jobs";

    public static void main(String[] args) {
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(webSocketHandler);
            Channel channel = bootstrap.bind(5001).sync().channel();
            channel.closeFuture().sync();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }


    public static void createJobInfoJSON(String newAppID) {
        File directory = new File(String.format("%s", dataPath));
        if (!directory.exists()) {
            if (directory.mkdirs()) {
                Log.info("Directory created successfully.");
            } else {
                Log.info("Failed to create directory.");
                return;
            }
        }

//        String newJobInfoFile = String.format("%s/%s.json", directory, newAppID);
        String newJobInfoFile = String.format("%s/%s.json", directory, "3");
        Path inputFile = Paths.get(newJobInfoFile);
        // create jobInfo json file for new job
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
        log.info("Job start time: " + jobStartTime);

        JSONOperator operator1 = new JSONOperator(
                "1", "SL", 4, -1, -1, "NA", "NA", "NA");
        List<JSONOperator> operators = new ArrayList<>();
        operators.add(operator1);

        JSONTimeBreakdown totalTimeBreakdown = new JSONTimeBreakdown(0, 0, 0, 0, 0);
        JSONSchedulerTimeBreakdown schedulerTimeBreakdown = new JSONSchedulerTimeBreakdown(0, 0, 0, 0, 0);

        JSONApplication application = new JSONApplication(
                "3", "Stream Ledger", 4, "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz", "16GB",
                jobStartTime, "0", false, 2500, 0, 0, 0,
                0, 0, 8, new ArrayList<>(), new ArrayList<>(),
                operators, totalTimeBreakdown, schedulerTimeBreakdown);
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            objectMapper.writeValue(new File(newJobInfoFile), application);
            log.info("JSON file created successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class JSONOperator {
        public String id;
        public String name;
        public int numOfInstances;
        public double throughput;
        public double latency;
        public String explorationStrategy;
        public String schedulingGranularity;
        public String abortHandling;

        public JSONOperator(String id, String name, int numOfInstances, double throughput, double latency,
                            String explorationStrategy, String schedulingGranularity, String abortHandling) {
            this.id = id;
            this.name = name;
            this.numOfInstances = numOfInstances;
            this.throughput = throughput;
            this.latency = latency;
            this.explorationStrategy = explorationStrategy;
            this.schedulingGranularity = schedulingGranularity;
            this.abortHandling = abortHandling;
        }
    }

    public static class JSONTimeBreakdown {
        public double totalTime;
        public double serializeTime;
        public double persistTime;
        public double streamProcessTime;
        public double overheadTime;
        public JSONTimeBreakdown(double totalTime, double serializeTime, double persistTime, double streamProcessTime, int overheadTime) {
            this.totalTime = totalTime;
            this.serializeTime = serializeTime;
            this.persistTime = persistTime;
            this.streamProcessTime = streamProcessTime;
            this.overheadTime = overheadTime;
        }
    }

    public static class JSONSchedulerTimeBreakdown {
        public int exploreTime;
        public int usefulTime;
        public double abortTime;
        public int constructTime;
        public int trackingTime;
        public JSONSchedulerTimeBreakdown(int exploreTime, int usefulTime, double abortTime, int constructTime, int trackingTime) {
            this.exploreTime = exploreTime;
            this.usefulTime = usefulTime;
            this.abortTime = abortTime;
            this.constructTime = constructTime;
            this.trackingTime = trackingTime;
        }
    }

    public static class JSONApplication {
        public String jobId;
        public String name;
        public int nthreads;
        public String cpu;
        public String ram;
        public String startTime;
        public String duration;
        public boolean isRunning;
        public int nevents;
        public long minProcessTime;
        public long maxProcessTime;
        public long meanProcessTime;
        public double latency;
        public double throughput;
        public int ncore;
        public List<Double> periodicalThroughput;
        public List<Double> periodicalLatency;
        public List<JSONOperator> operators;
        public JSONTimeBreakdown totalTimeBreakdown;
        public JSONSchedulerTimeBreakdown schedulerTimeBreakdown;
        public JSONApplication(String jobId, String name, int nthreads, String cpu, String ram, String startTime, String duration,
                               boolean isRunning, int nevents, long minProcessTime, long maxProcessTime, long meanProcessTime,
                               double latency, double throughput, int ncore, List<Double> periodicalThroughput, List<Double> periodicalLatency,
                               List<JSONOperator> operators, JSONTimeBreakdown totalTimeBreakdown, JSONSchedulerTimeBreakdown schedulerTimeBreakdown) {
            this.jobId = jobId;
            this.name = name;
            this.nthreads = nthreads;
            this.cpu = cpu;
            this.ram = ram;
            this.startTime = startTime;
            this.duration = duration;
            this.isRunning = isRunning;
            this.nevents = nevents;
            this.minProcessTime = minProcessTime;
            this.maxProcessTime = maxProcessTime;
            this.meanProcessTime = meanProcessTime;
            this.latency = latency;
            this.throughput = throughput;
            this.ncore = ncore;
            this.periodicalThroughput = periodicalThroughput;
            this.periodicalLatency = periodicalLatency;
            this.operators = operators;
            this.totalTimeBreakdown = totalTimeBreakdown;
            this.schedulerTimeBreakdown = schedulerTimeBreakdown;
        }
    }
    @Override
    public void run() {
        main(new String[0]);
    }
}
