package hbh.mysql3307;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import hbh.utils.HbaseUtil;
import hbh.utils.JsonUtil;
import hbh.utils.MysqlUtil;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Flink_cdc_inital {

    public static void main(String[] args) throws Exception {

        String hostname = "dw.3307.mysqlp.jhops.club";
        Integer port = 3307;
        String databaseList = "xianchang,xianchang_bj,xianchang_chengdu,xianchang_gz,xianchang_hive,xianchang_hz,xianchang_hzdata,xianchang_sh,xianchang_tj,xianchang_wh";
        String tableList = null;
        String username = "";
        String password = "";
        String job_name = "flink_3307_xianchang_city";

        Properties extralPro = new Properties();
        extralPro.setProperty("AllowPublicKeyRetrieval", "true");
        // extralPro.setProperty("database.history.store.only.monitored.tables.ddl","false");
        //      extralPro.setProperty("serverTimezone","Asia/Shanghai");
        String serverTimeZone = "Asia/Shanghai";
        String url = "jdbc:mysql://" + hostname + ":" + port + "?AllowPublicKeyRetrieval=true";
        SourceFunction<String> sourceFunction = MySqlSource.builder()
                .serverTimeZone(serverTimeZone)
                .hostname(hostname)
                .port(port)
                .databaseList(databaseList) // monitor all tables under inventory database //.tableList("123456"
                .username(username)
                .password(password)
                .debeziumProperties(extralPro)
                .deserializer(new JsonDebeziumDeserializationSchema(url, username, password, databaseList, tableList))
                .startupOptions(StartupOptions.initial())
                // .startupOptions(StartupOptions.specificOffset("mysql-bin.000006", 186203769))
                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().registerKryoType(SourceRecord.class);
        env.setParallelism(1);

        final DataStreamSource<String> source = env.addSource(sourceFunction);

        //  source.print("==>");


        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).addSink(new RichSinkFunction<String>() {
            Connection connection;
            Table flink_cdc;

            @Override
            public void open(Configuration parameters) throws Exception {


                org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
                configuration.addResource("xml/hbase-site.xml");
                configuration.addResource("xml/core-site.xml");
                configuration.addResource("xml/hdfs-site.xml");
                configuration.addResource("xml/yarn-site.xml");
                configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                connection = ConnectionFactory.createConnection(configuration, User.create(UserGroupInformation.createRemoteUser("hbase")));
                flink_cdc = connection.getTable(TableName.valueOf("FLINK:3307_xianchang_city_alltable"));

            }

            @Override
            public void close() throws Exception {
                if (null != flink_cdc) {
                    flink_cdc.close();
                }

                if (null != connection) {
                    connection.close();
                }
            }

            @Override
            public void invoke(String value, Context context) throws Exception {


                HashMap<String, String> value_map = JsonUtil.jsonToMap(value);
                HashMap<String, String> data_map = JsonUtil.jsonToMap(JsonUtil.jsonToMap(value).getOrDefault("data", ""));

                String key = value_map.getOrDefault("key", "");
                String d_t = value_map.getOrDefault("source", "").replace("mysql_binlog_source.", "").replace(".", "_");
                String row_key = d_t.concat("##").concat(key);
                Put put = new Put(row_key.getBytes());


                //  System.out.println("row_key " + row_key);

                if (value_map.getOrDefault("op", "").equals("+I")) {

                    for (Map.Entry<String, String> entry : data_map.entrySet()) {
                        put.addColumn("f".getBytes(), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                        //        System.out.println("key  " + d_t.concat("_").concat(entry.getKey()));


                    }

                    //    System.out.println("+I=======>" + value_map);

                    flink_cdc.put(put);
                } else if (value_map.getOrDefault("op", "").equals("-D")) {

                    //   System.out.println("-D=======>" + value_map);
                    HbaseUtil.delete_table_data(flink_cdc, row_key);
                } else if (value_map.getOrDefault("op", "").equals("+U")) {

                    for (Map.Entry<String, String> entry : data_map.entrySet()) {
                        put.addColumn("f".getBytes(), entry.getKey().getBytes(), entry.getValue().getBytes());
                    }

                    // System.out.println("+U=======>" + value_map);

                    flink_cdc.put(put);
                } else if (value_map.getOrDefault("op", "").equals("-U")) {

                    //  System.out.println("-U=======>" + value_map);
                    HbaseUtil.delete_table_data(flink_cdc, row_key);
                } else {
                    System.out.println("else=======>" + value_map);

                }


            }
        });


        env.execute(job_name);
    }


    public static class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema {

        private String url;
        private String username;
        private String password;
        private String databases;
        private String tables;
        Map<String, Map<String, String>> map;

        public JsonDebeziumDeserializationSchema(String url, String username, String password, String databases, String tables) {
            this.url = url;
            this.username = username;
            this.password = password;
            this.databases = databases;
            this.tables = tables;
            MysqlUtil mysqlUtil = new MysqlUtil(url, username, password);

            this.map = new HashMap<>();

            if (this.tables == null || this.tables.length() <= 1) {
                String[] dbArr = this.databases.split(",");
                for (String dbName : dbArr) {
                    List<String> tableNameList = mysqlUtil.getTableNames(dbName);
                    for (String tableName : tableNameList) {

                        String dbTableName = dbName + "." + tableName;

                        Map<String, String> colNameTpyeMap = mysqlUtil.getColNameTpye(dbTableName);
                        this.map.put(dbTableName, colNameTpyeMap);

                    }

                }


            } else {
                String[] dbTableArr = this.tables.split(",");

                for (String dbTableName : dbTableArr) {
                    Map<String, String> colNameTpyeMap = mysqlUtil.getColNameTpye(dbTableName);
                    this.map.put(dbTableName, colNameTpyeMap);

                }


            }
            mysqlUtil.closeConnection();

            System.out.println(this.map);

        }

        public JsonDebeziumDeserializationSchema() {
        }

        @Override
        public TypeInformation getProducedType() {
            return TypeInformation.of(String.class);
        }

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
            // System.out.println(sourceRecord);

            final Operation op = Envelope.operationFor(sourceRecord);
            final String source = sourceRecord.topic();
            Struct value = (Struct) sourceRecord.value();

            Struct key = (Struct) sourceRecord.key();

            if (key != null) {
                //  System.out.println("正常："+sourceRecord.toString());
                List<Field> idNames = key.schema().fields();
                // System.out.println(idNames);
                String keyStr = "";
                for (Field idName : idNames) {
                    keyStr = keyStr + key.get(idName) + "_";
                }
                if (keyStr.length() > 0)
                    keyStr = keyStr.substring(0, keyStr.length() - 1);

                final Schema schema = sourceRecord.valueSchema();

                if (op != Operation.CREATE && op != Operation.READ) {

                    if (op == Operation.DELETE) {
                        String data = extractBeforeData(value, schema, this.map);

                        String record = new JSONObject()
                                .fluentPut("source", source)
                                .fluentPut("data", data)
                                .fluentPut("op", RowKind.DELETE.shortString())
                                .fluentPut("key", keyStr)
                                .toJSONString();
                        collector.collect(record);

                    } else {
                        String beforeData = extractBeforeData(value, schema, this.map);
                        String beforeRecord = new JSONObject()
                                .fluentPut("source", source)
                                .fluentPut("data", beforeData)
                                .fluentPut("op", RowKind.UPDATE_BEFORE.shortString())
                                .fluentPut("key", keyStr)
                                .toJSONString();
                        collector.collect(beforeRecord);
                        String afterData = extractAfterData(value, schema, this.map);
                        String afterRecord = new JSONObject()
                                .fluentPut("source", source)
                                .fluentPut("data", afterData)
                                .fluentPut("op", RowKind.UPDATE_AFTER.shortString())
                                .fluentPut("key", keyStr)
                                .toJSONString();
                        collector.collect(afterRecord);

                    }
                } else {
                    String data = extractAfterData(value, schema, this.map);

                    String record = new JSONObject()
                            .fluentPut("source", source)
                            .fluentPut("data", data)
                            .fluentPut("op", RowKind.INSERT.shortString())
                            .fluentPut("key", keyStr)
                            .toJSONString();
                    collector.collect(record);
                }
            } else {
                System.out.println("异常数据（key为空） " + sourceRecord.toString());
            }

        }


        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getDatabases() {
            return databases;
        }

        public void setDatabases(String databases) {
            this.databases = databases;
        }

        public String getTables() {
            return tables;
        }

        public void setTables(String tables) {
            this.tables = tables;
        }
    }


    public static String extractBeforeData(Struct value, Schema schema, Map<String, Map<String, String>> map) {
        String dbTableName = value.getStruct("source").getString("db") + "." + value.getStruct("source").getString("table");

        Map<String, String> tableColNameTypeMap = map.getOrDefault(dbTableName, new HashMap<String, String>());

        final Struct before = value.getStruct("before");
        final List<Field> fields = before.schema().fields();
        JSONObject jsonObject = new JSONObject();
        for (Field field : fields) {
            //  System.out.println("field=> " + field.name() + "  " + before.get(field));
            String fileName = tableColNameTypeMap.getOrDefault(field.name(), field.name());
            if (fileName.equals("TIMESTAMP") && before.get(field) != null) {

                try {
                    long l = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(before.get(field).toString()).getTime() + 28800000;

                    String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(l));
                    jsonObject.put(field.name(), time);

                } catch (ParseException e) {
                    e.printStackTrace();
                }


            } else if (fileName.equals("DATETIME") && before.get(field) != null) {
                long l = Long.valueOf(before.get(field).toString()) - 28800000;
                String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(l));
                jsonObject.put(field.name(), time);

            } else {
                jsonObject.put(field.name(), before.get(field));
            }


        }
        return jsonObject.toJSONString();
    }

    public static String extractAfterData(Struct value, Schema schema, Map<String, Map<String, String>> map) {
        String dbTableName = value.getStruct("source").getString("db") + "." + value.getStruct("source").getString("table");

        Map<String, String> tableColNameTypeMap = map.getOrDefault(dbTableName, new HashMap<String, String>());


        final Struct after = value.getStruct("after");
        final List<Field> fields = after.schema().fields();
        JSONObject jsonObject = new JSONObject();
        for (Field field : fields) {
            //  System.out.println(field.name());
            String fileName = tableColNameTypeMap.getOrDefault(field.name(), field.name());

            //  System.out.println(fileName);
            //  System.out.println(after.get(field));

            // String k = "";
            //String v = "";

            if (fileName.equals("TIMESTAMP") && after.get(field) != null) {

                long l = 0;
                try {
                    l = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(after.get(field).toString()).getTime() + 28800000;
                    String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(l));

//                    k = field.name();
//                    v = time;
                    jsonObject.put(field.name(), time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }


            } else if (fileName.equals("DATETIME") && after.get(field) != null) {

                long l = Long.parseLong(after.get(field).toString()) - 28800000;
                String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(l));
                // k = field.name();
                // v = time;
                jsonObject.put(field.name(), time);

            } else {
                //  k = field.name();
                // v = after.get(field).toString();
                jsonObject.put(field.name(), after.get(field));
            }


            // jsonObject.put(k, v);

        }
        return jsonObject.toJSONString();
    }

}




