package com.wipro.analytics;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by cloudera on 3/19/17.
 */
public class HiveConnection {

    private static final String HIVE_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_USER = "cloudera";
    private static final String HIVE_PASSWORD = "cloudera";
    private static final String FILE_LINE_SEPERATOR = "\n";
    private static final String FILE_FIELD_SEPERATOR = "\t";
    private String DBNAME = "default";
    private String HIVE_CONNECTION_URL = "jdbc:hive2://localhost:10000/";

    public static Connection getHiveJDBCConnection(String dbName, String hiveConnection) throws SQLException {
        try {
            Class.forName(HIVE_DRIVER_NAME);
            String hiveUser = HIVE_USER;
            String hivePassword = HIVE_PASSWORD;
            Connection connection = DriverManager.getConnection(hiveConnection + "/" + dbName, hiveUser, hivePassword);
            /* con.createStatement().execute("set hive.exec.dynamic.partition.mode=nonstrict");
            con.createStatement().execute("set hive.exec.dynamic.partition=true");
            con.createStatement().execute("set hive.exec.max.dynamic.partitions.pernode=1000");*/
            return connection;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

    }


    public void loadIntoHive(){
        try {
            Connection conn = getHiveJDBCConnection(DBNAME,HIVE_CONNECTION_URL);
            Statement stmt = conn.createStatement();
            String query = "LOAD DATA INPATH '" + filename + "' INTO TABLE " + tableName + "')";
            stmt.executeUpdate(query);

            stmt.close();
            conn.close();

        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
