package com.wipro.analytics.fetchers;

import com.wipro.analytics.HiveConnection;
import com.wipro.analytics.beans.RunningJobsInfo;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Timestamp;
import java.util.Calendar;


/**
 * Created by cloudera on 3/15/17.
 */

public class RunningJobsFetcher implements Job {

    private final static String runningJobsFile = DataFetcherMain.RUNNING_JOBS_FILE;
    private final static String runningJobsAggregatedDir = DataFetcherMain.RUNNING_JOBS_AGGREGATED_DIR;
    private final static String resourceManagerHost = DataFetcherMain.RESOURCE_MANAGER_HOST;
    private final static String resourceManagerPort = DataFetcherMain.RESOURCE_MANAGER_PORT;
    private static final long scheduleInterval = DataFetcherMain.SCHEDULE_INTERVAL;
    private static final long aggregationInterval = DataFetcherMain.AGGREGATION_INTERVAL;
    private static final String lineSeparator = DataFetcherMain.FILE_LINE_SEPERATOR;
    private static final String runningJobsTable = DataFetcherMain.RUNNING_JOBS_TABLE;
    static int counter = 0;
    static int aggregateCounter = 0;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RunningJobsFetcher() {
    }

    public JsonNode readJsonNode(URL url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        return objectMapper.readTree(conn.getInputStream());
    }

    public void execute(JobExecutionContext context)
            throws JobExecutionException {
        try {
            URL runningAppsUrl = new URL("http://" + resourceManagerHost + ":" + resourceManagerPort + "/ws/v1/cluster/apps?states=running");
            JsonNode rootNode = readJsonNode(runningAppsUrl);
            JsonNode apps = rootNode.path("apps").path("app");
            BufferedWriter writer = new BufferedWriter(new FileWriter(runningJobsFile, true));
            counter++;
            for (JsonNode app : apps) {
                String applicationId = app.get("id").asText();
                String applicationName = app.get("name").asText();
                String applicationState = app.get("state").asText();
                String applicationType = app.get("applicationType").asText();
                String finalState = app.get("finalStatus").asText();
                String progress = app.get("progress").asText();
                String username = app.get("user").asText();
                String queueName = app.get("queue").asText();
                long startTime = app.get("startedTime").getLongValue();
                long elapsedTime = app.get("elapsedTime").getLongValue();
                long finishTime = app.get("finishedTime").getLongValue();
                String trackingUrl = app.get("trackingUrl") != null ? app.get("trackingUrl").asText() : null;
                int numContainers = app.get("runningContainers").getIntValue();
                int allocatedMB = app.get("allocatedMB").getIntValue();
                int allocatedVCores = app.get("allocatedVCores").getIntValue();
                long memorySeconds = app.get("memorySeconds").getLongValue();
                long vcoreSeconds = app.get("vcoreSeconds").getLongValue();

                RunningJobsInfo runningJobsInfo = new RunningJobsInfo();
                runningJobsInfo.setApplicationId(applicationId);
                runningJobsInfo.setApplicationName(applicationName);
                runningJobsInfo.setApplicationState(applicationState);
                runningJobsInfo.setApplicationType(applicationType);
                runningJobsInfo.setFinalState(finalState);
                runningJobsInfo.setProgress(progress);
                runningJobsInfo.setUsername(username);
                runningJobsInfo.setQueueName(queueName);
                runningJobsInfo.setStartTime(startTime);
                runningJobsInfo.setElapsedTime(elapsedTime);
                runningJobsInfo.setFinishTime(finishTime);
                runningJobsInfo.setTrackingUrl(trackingUrl);
                runningJobsInfo.setNumContainers(numContainers);
                runningJobsInfo.setAllocatedMB(allocatedMB);
                runningJobsInfo.setAllocatedVCores(allocatedVCores);
                runningJobsInfo.setMemorySeconds(memorySeconds);
                runningJobsInfo.setVcoreSeconds(vcoreSeconds);

                //write this runningjobinfo to file

                runningJobsInfo.setTimestamp(new Timestamp(Calendar.getInstance().getTime().getTime()));
                writer.write(runningJobsInfo.toString() + lineSeparator);

            }
            writer.close();
            System.out.println("running counter = " + counter);
            if (counter == aggregationInterval / scheduleInterval) {
                counter = 0;
                if (new File(runningJobsFile).length() != 0) {
                    aggregateCounter++;
                    String fileName = runningJobsAggregatedDir + "running-" + System.currentTimeMillis();
                    Files.copy(new File(runningJobsFile).toPath(), new File(fileName).toPath(), StandardCopyOption.REPLACE_EXISTING);
                    PrintWriter pw = new PrintWriter(runningJobsFile);
                    pw.close();
                    HiveConnection hiveConnection = new HiveConnection();
                    hiveConnection.loadIntoHive(fileName, runningJobsTable);
                }
            }

        } catch (Exception e) {
            System.out.println("e = " + e);
            e.printStackTrace();
        }
    }


}