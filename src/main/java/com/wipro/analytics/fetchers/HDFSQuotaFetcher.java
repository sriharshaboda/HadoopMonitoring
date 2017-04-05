package com.wipro.analytics.fetchers;

import com.wipro.analytics.HiveConnection;
import com.wipro.analytics.beans.HDFSQuotaInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Created by cloudera on 4/3/17.
 */
@DisallowConcurrentExecution
public class HDFSQuotaFetcher implements Job {
    private final static String hdfsQuotaFile = DataFetcherMain.HDFS_QUOTA_FILE;
    private final static String hdfsQuotaAggregatedDir = DataFetcherMain.HDFS_QUOTA_AGGREGATED_DIR;
    private static final String hdfsQuotaTable = DataFetcherMain.HDFS_QUOTA_TABLE;
    private static final long scheduleInterval = DataFetcherMain.SCHEDULE_INTERVAL;
    private static final long aggregationInterval = DataFetcherMain.AGGREGATION_INTERVAL;
    private static final String lineSeparator = DataFetcherMain.FILE_LINE_SEPERATOR;
    private static final String NAME_NODE_HOST = DataFetcherMain.NAMENODE_HOST;
    private static final String NAME_NODE_PORT = DataFetcherMain.NAMENODE_PORT;
    private static final String FOLDERS_TO_MONITOR_HDFS_QUOTA = DataFetcherMain.FOLDERS_TO_MONITOR_FOR_HDFS_QUOTA;

    static int counter = 0;
    static int aggregateCounter = 0;

    public HDFSQuotaFetcher() {
    }

    public void execute(JobExecutionContext context)
            throws JobExecutionException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://" + NAME_NODE_HOST + ":" + NAME_NODE_PORT);
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(hdfsQuotaFile, true));
            counter++;
            FileSystem fs = FileSystem.get(conf);
            for (String hdfsPath : FOLDERS_TO_MONITOR_HDFS_QUOTA.split(",")) {
                ContentSummary contentSummary = fs.getContentSummary(new Path(hdfsPath));
                HDFSQuotaInfo hdfsQuotaInfo = new HDFSQuotaInfo();
                hdfsQuotaInfo.setHdfsPath(hdfsPath);
                long quota = contentSummary.getQuota();
                hdfsQuotaInfo.setQuota(quota);
                long spaceQuota = contentSummary.getSpaceQuota();
                hdfsQuotaInfo.setSpaceQuota(spaceQuota);
                long spaceConsumed = contentSummary.getSpaceConsumed();
                hdfsQuotaInfo.setSpaceConsumed(spaceConsumed);
                long numFiles = contentSummary.getDirectoryCount() + contentSummary.getFileCount();
                hdfsQuotaInfo.setNumFiles(numFiles);

                hdfsQuotaInfo.setTimestamp(new Timestamp(Calendar.getInstance().getTime().getTime()));
                System.out.println("hdfsQuotaInfo = " + hdfsQuotaInfo.toString());
                writer.write(hdfsQuotaInfo.toString() + lineSeparator);

            }
            writer.close();
            System.out.println("hdfs quota counter = " + counter);
            if (counter == aggregationInterval / scheduleInterval) {
                counter = 0;
                if (new File(hdfsQuotaFile).length() != 0) {
                    aggregateCounter++;
                    String fileName = hdfsQuotaAggregatedDir + "hdfsquota-" + System.currentTimeMillis();
                    Files.copy(new File(hdfsQuotaFile).toPath(), new File(fileName).toPath(), StandardCopyOption.REPLACE_EXISTING);
                    PrintWriter pw = new PrintWriter(hdfsQuotaFile);
                    pw.close();
                    HiveConnection hiveConnection = new HiveConnection();
                    hiveConnection.loadIntoHive(fileName, hdfsQuotaTable);
                }
            }
        } catch (Exception e) {
            System.out.println("e = " + e);
        }

    }
}
