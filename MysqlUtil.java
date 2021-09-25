package hbh.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlUtil implements Serializable {

    private final static Logger LOGGER = LoggerFactory.getLogger(MysqlUtil.class);
    private static final String SQL = "SELECT * FROM ";// 数据库操作
    private static final String DRIVER = "com.mysql.jdbc.Driver";


    private String url = "jdbc:mysql://192.168.10.129:3306?useUnicode=true&characterEncoding=utf8";
    private String usename = "root";
    private String password = "123456";
    private Connection conn;

    public MysqlUtil(String url, String usename, String password) {
        this.url = url;
        this.usename = usename;
        this.password = password;

        try {
            Class.forName(DRIVER);
            this.conn = DriverManager.getConnection(url, usename, password);
        } catch (ClassNotFoundException | SQLException e) {
            LOGGER.error("can not load jdbc driver", e);
        }
    }


    /**
     * 获取数据库连接
     *
     * @return
     */
    public Connection getConnection() {
        return this.conn;
    }

    /**
     * 关闭数据库连接
     *
     * @param
     */
    public void closeConnection() {
        if (this.conn != null) {
            try {
                this.conn.close();
            } catch (SQLException e) {
                LOGGER.error("close connection failure", e);
            }
        }
    }

    /**
     * 获取数据库下的所有表名
     */
    public List<String> getTableNames(String dbName) {
        List<String> tableNames = new ArrayList<>();
        ResultSet rs = null;
        try {
            //获取数据库的元数据
            DatabaseMetaData db = conn.getMetaData();
            //从元数据中获取到所有的表名

            rs = db.getTables(dbName, null, null, new String[]{"TABLE"});
            while (rs.next()) {
                tableNames.add(rs.getString(3));
            }
        } catch (SQLException e) {
            LOGGER.error("getTableNames failure", e);
        } finally {
            try {
                rs.close();
            } catch (SQLException e) {
                LOGGER.error("close ResultSet failure", e);
            }
        }
        return tableNames;
    }

    /**
     * 获取表中所有字段名称
     *
     * @param dbTbaleName 表名
     * @return
     */
    public List<String> getColumnNames(String dbTbaleName) {
        List<String> columnNames = new ArrayList<>();
        //与数据库的连接
        PreparedStatement pStemt = null;
        String tableSql = SQL + dbTbaleName;
        try {
            pStemt = conn.prepareStatement(tableSql);
            //结果集元数据
            ResultSetMetaData rsmd = pStemt.getMetaData();
            //表列数
            int size = rsmd.getColumnCount();
            for (int i = 0; i < size; i++) {
                columnNames.add(rsmd.getColumnName(i + 1));
            }
        } catch (SQLException e) {
            LOGGER.error("getColumnNames failure", e);
        } finally {
            if (pStemt != null) {
                try {
                    pStemt.close();
                } catch (SQLException e) {
                    LOGGER.error("getColumnNames close pstem and connection failure", e);
                }
            }
        }
        return columnNames;
    }

    /**
     * 获取表中所有字段类型
     *
     * @param dbTbaleName db.table
     * @return
     */
    public List<String> getColumnTypes(String dbTbaleName) {
        List<String> columnTypes = new ArrayList<>();
        //与数据库的连接
        PreparedStatement pStemt = null;
        String tableSql = SQL + dbTbaleName;
        try {
            pStemt = conn.prepareStatement(tableSql);
            //结果集元数据
            ResultSetMetaData rsmd = pStemt.getMetaData();
            //表列数
            int size = rsmd.getColumnCount();
            for (int i = 0; i < size; i++) {
                columnTypes.add(rsmd.getColumnTypeName(i + 1));
            }
        } catch (SQLException e) {
            LOGGER.error("getColumnTypes failure", e);
        } finally {
            if (pStemt != null) {
                try {
                    pStemt.close();
                } catch (SQLException e) {
                    LOGGER.error("getColumnTypes close pstem and connection failure", e);
                }
            }
        }
        return columnTypes;
    }


    //获取表 datetime timestamp俩种类型的 map（字段名，字段类型）
    public Map<String, String> getColNameTpye(String dbTbaleName) {
        HashMap<String, String> map = new HashMap<>();

        List<String> colNameList = getColumnNames(dbTbaleName);
        List<String> colTypeList = getColumnTypes(dbTbaleName);

        if (colNameList.size() == colTypeList.size()) {

            for (int i = 0; i < colNameList.size(); i++) {
                if (colTypeList.get(i).toUpperCase().equals("TIMESTAMP") || colTypeList.get(i).toUpperCase().equals("DATETIME"))
                    map.put(colNameList.get(i), colTypeList.get(i));
            }

        } else {
            LOGGER.error(dbTbaleName + " 元数据信息异常");
        }
        return map;


    }

    /**
     * 获取表中字段的所有注释
     *
     * @param tableName
     * @return
     */
    public List<String> getColumnComments(String tableName) {
        List<String> columnTypes = new ArrayList<>();
        //与数据库的连接
        PreparedStatement pStemt = null;
        String tableSql = SQL + tableName;
        List<String> columnComments = new ArrayList<>();//列名注释集合
        ResultSet rs = null;
        try {
            pStemt = conn.prepareStatement(tableSql);
            rs = pStemt.executeQuery("show full columns from " + tableName);
            while (rs.next()) {
                columnComments.add(rs.getString("Comment"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    LOGGER.error("getColumnComments close ResultSet and connection failure", e);
                }
            }
        }
        return columnComments;
    }

    public static void main(String[] args) {

        MysqlUtil mysqlUtil = new MysqlUtil("jdbc:mysql://192.168.10.129:3306?useUnicode=true&characterEncoding=utf8", "root", "123456");

        System.out.println(mysqlUtil.getTableNames("test"));

        System.out.println(mysqlUtil.getColNameTpye("test2.order"));

    }
}

