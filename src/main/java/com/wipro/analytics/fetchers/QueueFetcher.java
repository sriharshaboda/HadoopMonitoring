package com.wipro.analytics.fetchers;

import com.wipro.analytics.HiveConnection;
import com.wipro.analytics.beans.QueueInfo;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Created by cloudera on 3/18/17.
 */

@DisallowConcurrentExecution
public class QueueFetcher implements Job {
    private final static String resourceManagerHost = DataFetcherMain.RESOURCE_MANAGER_HOST;
    private final static String resourceManagerPort = DataFetcherMain.RESOURCE_MANAGER_PORT;
    private static final long scheduleInterval = DataFetcherMain.SCHEDULE_INTERVAL;
    private static final long aggregationInterval = DataFetcherMain.AGGREGATION_INTERVAL;
    private static final String lineSeparator = DataFetcherMain.FILE_LINE_SEPERATOR;
    private static final String queueTable = DataFetcherMain.QUEUE_TABLE;
    static int counter = 0;
    static int aggregateCounter = 0;
    private static List<QueueInfo> queueInfoList = new ArrayList<QueueInfo>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String queuesFile = DataFetcherMain.QUEUES_FILE;
    private final String queuesAggregatedDir = DataFetcherMain.QUEUES_AGGREGATED_DIR;

    public QueueFetcher() {
    }

    public JsonNode readJsonNode(URL url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        return objectMapper.readTree(conn.getInputStream());
    }

    public void execute(JobExecutionContext context)
            throws JobExecutionException {
        try {
            URL schedulerUrl = new URL("http://" + resourceManagerHost + ":" + resourceManagerPort + "/ws/v1/cluster/scheduler");
            JsonNode rootNode = readJsonNode(schedulerUrl);
            JsonNode schedulerInfo = rootNode.path("scheduler").path("schedulerInfo");
            String schedulerType = schedulerInfo.get("type").asText();
            //   System.out.println("schedulerType = " + schedulerType);

            if (schedulerType.equalsIgnoreCase("fairScheduler")) {

            } else if (schedulerType.equalsIgnoreCase("capacityScheduler")) {
                queueInfoList.clear();
                getCapacitySchedulerQueue(schedulerInfo);
                BufferedWriter writer = new BufferedWriter(new FileWriter(queuesFile, true));
                //System.out.println("queuinfo list size = " + queueInfoList.size());
                for (QueueInfo queueInfo : queueInfoList) {
                    queueInfo.setTimestamp(new Timestamp(Calendar.getInstance().getTime().getTime()));
                    writer.write(queueInfo.toString() + lineSeparator);
                }
                writer.close();
                counter++;
                System.out.println("queue counter =" + counter);
                if (counter == aggregationInterval / scheduleInterval) {
                    counter = 0;
                    if (new File(queuesFile).length() != 0) {
                        aggregateCounter++;
                        String fileName = queuesAggregatedDir + "queue-" + System.currentTimeMillis();
                        Files.copy(new File(queuesFile).toPath(), new File(fileName).toPath(), StandardCopyOption.REPLACE_EXISTING);
                        PrintWriter pw = new PrintWriter(queuesFile);
                        pw.close();
                        HiveConnection hiveConnection = new HiveConnection();
                        hiveConnection.loadIntoHive(fileName, queueTable);
                    }
                }

            }

        } catch (Exception e) {
            System.out.println("e = " + e);
            e.printStackTrace();
        }
    }

    public void getCapacitySchedulerQueue(final JsonNode node) throws IOException {
        JsonNode queueArray = node.path("queues").path("queue");
        for (JsonNode queue : queueArray) {

            //queues fetched here contain all queues
            if (queue.has("queues")) {
                getCapacitySchedulerQueue(queue);
            } else {

                //queues fetched here are only leaf queues
                String queueName = queue.get("queueName").asText();
                double absoluteAllocatedCapacity = queue.get("absoluteCapacity").getDoubleValue();
                double absoluteUsedCapacity = queue.get("absoluteUsedCapacity").getDoubleValue();

                //// TODO: 3/20/2017 know diff between resourcesUsed & usedResources both in same json
                JsonNode resourcesUsed = queue.path("resourcesUsed");
                int usedMemory = resourcesUsed.get("memory").getIntValue();
                int usedCores = resourcesUsed.get("vCores").getIntValue();


                int numContainers = queue.get("numContainers").getIntValue();
                String queueState = queue.get("state").asText();

                //Below elements of queue are present only in leaf queues but not in parent queue
                int maxApplications = queue.get("maxApplications").getIntValue();
                int numApplications = queue.get("numApplications").getIntValue();
                int numActiveApplications = queue.get("numActiveApplications").getIntValue();
                int numPendingApplications = queue.get("numPendingApplications").getIntValue();
                String queueType = queue.get("type").asText();

                //Iterate through users json array
                String users = "";
                JsonNode usersArray = queue.path("users").path("user");
                if (!usersArray.isMissingNode()) {
                    for (JsonNode user : usersArray) {
                        users = users + (user.get("username").asText()) + ",";
                    }
                    if (users.charAt(users.length() - 1) == ',') {
                        users = users.substring(0, users.length() - 1);
                    }
                }

                QueueInfo queueInfo = new QueueInfo();
                queueInfo.setQueueName(queueName);
                queueInfo.setAbsoluteUsedCapacity(absoluteUsedCapacity);
                queueInfo.setAbsoluteAllocatedCapacity(absoluteAllocatedCapacity);
                queueInfo.setUsedMemory(usedMemory);
                queueInfo.setUsedCores(usedCores);
                queueInfo.setNumContainers(numContainers);
                queueInfo.setQueueState(queueState);
                queueInfo.setNumApplications(numApplications);
                queueInfo.setNumActiveApplications(numActiveApplications);
                queueInfo.setNumPendingApplications(numPendingApplications);
                queueInfo.setQueueType(queueType);
                queueInfo.setUsers(users);
                queueInfoList.add(queueInfo);

            }
        }
    }
}
