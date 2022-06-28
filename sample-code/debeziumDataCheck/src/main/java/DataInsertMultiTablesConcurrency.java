

import org.apache.pulsar.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

public class DataInsertMultiTablesConcurrency {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //构造线程池
    private static ExecutorService threadPool = new ThreadPoolExecutor(2, 2, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(100, true),
            new ThreadFactoryBuilder().setNameFormat("Multi Tables Insert Pool-thread-%d").build(),
            new ThreadPoolExecutor.AbortPolicy());

    //启动方法
    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {

        Connection conn = getConnection();

//        String tableNamePrefix= args[0];
//        int tableNumber= Integer.parseInt(args[1]);
//        int rowNumber = Integer.parseInt(args[2]);

        int tableNumber = 1000;
        int rowNumber = 1000;

        String tableNamePrefix = "debezium_testee";

        System.out.println("**************** Start Task");
        DataInsertMultiTablesConcurrency task = new DataInsertMultiTablesConcurrency();
        task.insertMultiTable(tableNamePrefix,tableNumber,rowNumber,conn);
        task.updateMultiTable(tableNamePrefix,tableNumber,rowNumber,conn);
        task.deleteMultiTable(tableNamePrefix,tableNumber,rowNumber,conn);
        Objects.requireNonNull(conn).close();
        System.out.println("**************** Task End Success");
    }


    //insertMultiTable
    public void insertMultiTable(String tableNamePrefix, int tableNumber, int rowNumber, Connection conn) throws SQLException, ExecutionException, InterruptedException {

        ArrayList<String> tableList = new ArrayList<String>();
        for (int i = 1; i <= tableNumber; i++) {
            tableList.add(tableNamePrefix + i);
        }
        for (String table : tableList) {
            createTable(conn, table);
        }
        List<Future<Void>> futureList = new ArrayList<>();
        //begin insert
        for (String table : tableList) {
            Statement statement = conn.createStatement();
            Future<Void> futureResSet = threadPool.submit(new insertCallable(statement, tableNamePrefix, table, tableNumber, rowNumber));
            futureList.add(futureResSet);
        }

        for(Future<Void> future : futureList){
            future.get();
        }

    }
    class insertCallable implements Callable<Void> {
        private Statement statement;
        private String tableNamePrefix;
        private String table;
        private int tableNumber;
        private int rowNumber;

        public insertCallable(Statement statement, String tableNamePrefix, String table, int tableNumber, int rowNumber) {
            this.statement = statement;
            this.tableNamePrefix = tableNamePrefix;
            this.table = table;
            this.tableNumber = tableNumber;
            this.rowNumber = rowNumber;
        }

        @Override
        public Void call() {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO " + table + " VALUES ");
            int num = 1;
            boolean needInsert = false;
            for (int i = 0; i < rowNumber; i++) {
                String province = "jiangsu_" + (i % 30);
                sb.append("(")
                        .append(i).append(",")
                        .append("\"").append(province).append("\",")
                        .append("\"").append("beijin_" + i).append("\",")
                        .append(1).append(",")
                        .append("\"").append(getStrFromDate(new Date())).append("\",")
                        .append("\"").append(getStrFromDate(new Date())).append("\",")
                        .append(i * 2).append(",")
                        .append("\'").append(1).append("\',")
                        .append("\"").append(System.currentTimeMillis()).append("\"")
                        .append("),");
                needInsert = true;
                if ((i + 1) % num == 0) {
                    System.out.println(i);
                    try {
                        statement.execute(sb.substring(0, sb.length() - 1));
                    } catch (SQLException e) {
                        System.out.println(e.getMessage());
                    }
                    sb = new StringBuilder("INSERT INTO " + table + " VALUES ");
                    needInsert = false;
                }
            }
            if (needInsert) {
                try {
                    statement.execute(sb.toString().substring(0, sb.length() - 1));
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }
            }
            try {
                statement.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            return null;
        }

    }

    //updateMultiTable
    public void updateMultiTable(String tableNamePrefix, int tableNumber, int rowNumber, Connection conn) throws SQLException, ExecutionException, InterruptedException {
        ArrayList<String> tableList = new ArrayList<String>();
        for (int i = 1; i <= tableNumber; i++) {
            tableList.add(tableNamePrefix + i);
        }

        List<Future<Void>> futureList = new ArrayList<>();
        //begin update
        for (String table : tableList) {
            Statement statement = conn.createStatement();
            Future<Void> futureResSet = threadPool.submit(new updateCallable(statement, tableNamePrefix, table, tableNumber, rowNumber));
            futureList.add(futureResSet);
        }
        for(Future<Void> future : futureList){
            future.get();
        }

    }

    class updateCallable implements Callable<Void> {
        private Statement statement;
        private String tableNamePrefix;
        private String table;
        private int tableNumber;
        private int rowNumber;

        public updateCallable(Statement statement, String tableNamePrefix, String table, int tableNumber, int rowNumber) {
            this.statement = statement;
            this.tableNamePrefix = tableNamePrefix;
            this.table = table;
            this.tableNumber = tableNumber;
            this.rowNumber = rowNumber;
        }

        @Override
        public Void call() {
            int num = 100;
            boolean needInsert = false;
            for (int i = 0; i < rowNumber; i++) {
                StringBuilder sb = new StringBuilder();
                sb.append("UPDATE " + table )
                        .append(" SET " )
                        .append(" create_time = ").append(System.currentTimeMillis())
                        .append(" WHERE id = ").append(i);

                System.out.println(sb.toString());
                try {
                    statement.execute(sb.toString());
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }
            }
            try {
                statement.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            return null;
        }
    }

    //deleteMultiTable
    public void deleteMultiTable(String tableNamePrefix,int tableNumber,int rowNumber,Connection conn) throws SQLException, ExecutionException, InterruptedException {
        ArrayList<String> tableList = new ArrayList<String>();
        for(int i = 1;i<= tableNumber; i ++) {
            tableList.add(tableNamePrefix+i);
        }
        List<Future<Void>> futureList = new ArrayList<>();

        //begin delete
        for (String table : tableList) {
            Statement statement = conn.createStatement();
            Future<Void> futureResSet = threadPool.submit(new deleteCallable(statement, tableNamePrefix, table, tableNumber, rowNumber));
            futureList.add(futureResSet);
        }
        for(Future<Void> future : futureList){
            future.get();
        }

    }

    class deleteCallable implements Callable<Void> {
        private Statement statement;
        private String tableNamePrefix;
        private String table;
        private int tableNumber;
        private int rowNumber;

        public deleteCallable(Statement statement, String tableNamePrefix, String table, int tableNumber, int rowNumber) {
            this.statement = statement;
            this.tableNamePrefix = tableNamePrefix;
            this.table = table;
            this.tableNumber = tableNumber;
            this.rowNumber = rowNumber;
        }

        @Override
        public Void call() {
            int num =100;
            boolean needInsert = false;
            for(int i = 0 ;i < rowNumber ; i++) {
                StringBuilder sb = new StringBuilder();
                sb.append("DELETE from " + table )
                        .append(" WHERE id = ").append(i);

                System.out.println(sb.toString());
                try {
                    statement.execute(sb.toString());
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }

            }
            try {
                statement.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            return null;
        }
    }


    //createTable
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

    //get db connection
    public static Connection getConnection() {
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
    //get date format
    public static String getStrFromDate(Date signInDate) {
        return sdf.format(signInDate);
    }
}

