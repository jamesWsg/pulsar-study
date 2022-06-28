
import org.apache.pulsar.client.api.PulsarClientException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DataInsertMultiTables {
    public static Connection getConnecttion() {
        Connection conn = null;
        try {
            String url = "jdbc:mysql://192.168.1.2:3306/test";
            String sql_user = "user";
            String sql_pwd = "password";
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, sql_user, sql_pwd);
            System.out.println("Database connection established");
            return conn;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static String getStrFromDate(Date signInDate) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(signInDate);
    }

    public static void createTable(Connection conn, String table ) throws SQLException {
        String creatTableSql = "(\n" +
                "    id INT(11) PRIMARY KEY,\n" +
                "    province VARCHAR(255),\n" +
                "    city VARCHAR(255),\n" +
                "    type INT,\n" +
                "    enddate DATETIME,\n" +
                "    pubdate DATETIME,\n" +
                "    ratio INT,\n" +
                "    unit CHAR(255),\n" +
                "    create_time BIGINT\n" +
                ")";
        Statement statement = conn.createStatement();
        String sql = "CREATE TABLE If Not Exists  " + table + creatTableSql;
        boolean rs = statement.execute(sql);
        statement.execute("delete  from " + table);
        statement.close();
        System.out.println(sql);
    }

    public static void insertMultiTable(String tableNamePrefix,int tableNumber,int rowNumber,Connection conn) throws SQLException {
        Statement statement = conn.createStatement();

        ArrayList<String> tableList = new ArrayList<String>();
        for(int i = 1;i<= tableNumber; i ++) {
            tableList.add(tableNamePrefix+i);
        }
        for (String table: tableList) {
            createTable(conn,table);
        }
        //begin insert
        for (String table: tableList){
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO " + table + " VALUES " );
            int num =100;
            boolean needInsert = false;
            for(int i = 0 ;i < rowNumber ; i++) {
                String province = "jiangsu_" + (i % 30);
                sb.append("(" )
                        .append(i).append(",")
                        .append("\"").append(province).append("\",")
                        .append("\"").append("beijin_" + i).append("\",")
                        .append(1).append(",")
                        .append("\"").append(getStrFromDate(new Date())).append("\",")
                        .append("\"").append(getStrFromDate(new Date())).append("\",")
                        .append(i*2).append(",")
                        .append("\'").append(1).append("\',")
                        .append("\"").append(System.currentTimeMillis()).append("\"")
                        .append("),");
                needInsert = true;   //??
                if ((i + 1) % num == 0) {
                    System.out.println(i);
                    statement.execute(sb.toString().substring(0, sb.length() - 1));
                    sb = new StringBuilder("INSERT INTO " + table + " VALUES ");
                    needInsert = false;
                }
            }
            if(needInsert == true) {
                statement.execute(sb.toString().substring(0, sb.length() - 1));
            }
        }

    }

    public static void updateMultiTable(String tableNamePrefix,int tableNumber,int rowNumber,Connection conn) throws SQLException {
        Statement statement = conn.createStatement();
        ArrayList<String> tableList = new ArrayList<String>();
        for(int i = 1;i<= tableNumber; i ++) {
            tableList.add(tableNamePrefix+i);
        }

        for (String table: tableList){

            int num =100;
            boolean needInsert = false;
            for(int i = 0 ;i < rowNumber ; i++) {
                StringBuilder sb = new StringBuilder();
                sb.append("UPDATE " + table )
                        .append(" SET " )
                        .append(" create_time = ").append(System.currentTimeMillis())
                        .append(" WHERE id = ").append(i);


                //System.out.println(sb.toString());
                statement.execute(sb.toString());

            }
        }

    }

    public static void deleteMultiTable(String tableNamePrefix,int tableNumber,int rowNumber,Connection conn) throws SQLException {
        Statement statement = conn.createStatement();
        ArrayList<String> tableList = new ArrayList<String>();
        for(int i = 1;i<= tableNumber; i ++) {
            tableList.add(tableNamePrefix+i);
        }

        for (String table: tableList){

            int num =100;
            boolean needInsert = false;
            for(int i = 0 ;i < rowNumber ; i++) {
                StringBuilder sb = new StringBuilder();
                sb.append("DELETE from " + table )
                        .append(" WHERE id = ").append(i);

                System.out.println(sb.toString());
                statement.execute(sb.toString());

            }
        }

    }

    public static void dropMultiTable(String tableNamePrefix,int tableNumber,int rowNumber,Connection conn) throws SQLException {
        Statement statement = conn.createStatement();
        ArrayList<String> tableList = new ArrayList<String>();
        for(int i = 1;i<= tableNumber; i ++) {
            tableList.add(tableNamePrefix+i);
        }

        for (String table: tableList){

            int num =100;
            boolean needInsert = false;
            StringBuilder sb = new StringBuilder();
            sb.append("DROP table " + table );
            System.out.println(sb.toString());
            statement.execute(sb.toString());


        }

    }

    public static void main(String[] args) throws PulsarClientException, SQLException {
        Connection conn = getConnecttion();
        String tableNamePrefix = "debezium_testee";
//        String tableNamePrefix = "testdd";
        int tableNumber = 1000;
        int rowNumber = 100000;
        insertMultiTable(tableNamePrefix,tableNumber,rowNumber,conn);
        updateMultiTable(tableNamePrefix,tableNumber,rowNumber,conn);
        deleteMultiTable(tableNamePrefix,tableNumber,rowNumber,conn);
//        dropMultiTable(tableNamePrefix,1000,1000,conn);

        conn.close();




    }
}

