package sourcecode.analysis;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.apache.flink.table.api.Expressions.row;

/** */
public class WriteToTable {

    //
    private static final String basePath = "/d:/incubator-paimon/paimon-source-analysis";
    //
    private static final String table = "sink_paimon_table";

    @Test
    public void streamWriteTo() throws ExecutionException, InterruptedException {
        StreamEnv streamEnv = new StreamEnv(basePath);
        // create paimon catalog
        streamEnv.tableEnv.executeSql(
                String.format(
                        "CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='file://%s/paimon_datalake')",
                        basePath));
        streamEnv.tableEnv.executeSql("USE CATALOG paimon");

        // register the table under a name and perform an aggregation

        streamEnv.tableEnv.executeSql(
                "CREATE TEMPORARY TABLE IF NOT EXISTS input_table (pk INT, name STRING, age INT, dt INT)"
                        + "WITH ("
                        + "'connector' = 'datagen',"
                        + "'rows-per-second' = '10',"
                        + "'number-of-rows' = '2000',"
                        + "'fields.name.length' = '10',"
                        + "'fields.pk.min' = '100001',"
                        + "'fields.pk.max' = '100021',"
                        + "'fields.dt.min' = '20230819',"
                        + "'fields.dt.max' = '20230820')");
        // create paimon table
        streamEnv.tableEnv.executeSql(
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s "
                                + "(pk INT, name STRING, age INT, dt INT, primary key(pk, dt) not enforced)"
                                + "partitioned by (dt) with (\n"
                                + "    'changelog-producer' = 'input',\n"
                                + "    'merge-engine'='deduplicate',\n"
                                + "    'bucket' = '1'\n"
                                + ")",
                        table));
        // insert into paimon table from your data stream table
        streamEnv
                .tableEnv
                .executeSql(String.format("INSERT INTO %s SELECT * FROM input_table", table))
                .await();
    }

    @Test
    public void streamNetcatWriteTo() throws Exception {
        StreamEnv streamEnv = new StreamEnv(basePath);
        // create paimon catalog
        streamEnv.tableEnv.executeSql(
                String.format(
                        "CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='file://%s/paimon_datalake')",
                        basePath));
        streamEnv.tableEnv.executeSql("USE CATALOG paimon");

        // register the table under a name and perform an aggregation
        DataStream<Row> source = streamEnv.senv.socketTextStream("localhost", 9527)
                .map(e -> {
                    String[] arr = e.split(",");
                    Integer pk = Integer.parseInt(arr[0]);
                    String name = arr[1];
                    Integer age = Integer.parseInt(arr[2]);
                    Integer dt = Integer.parseInt(arr[3]);
                    return Row.of(pk, name, age, dt);
                })
                .returns(Types.ROW_NAMED(
                        new String[]{"pk", "name", "age", "dt"},
                        Types.INT, Types.STRING, Types.INT, Types.INT
                ));
        // create paimon table
        streamEnv.tableEnv.executeSql(
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s "
                                + "(pk INT, name STRING, age INT, dt INT, primary key(pk, dt) not enforced)"
                                + "partitioned by (dt) with (\n"
                                + "    'changelog-producer' = 'input',\n"
                                + "    'merge-engine'='deduplicate',\n"
                                + "    'bucket' = '1'\n"
                                + ")",
                        table));
        // insert into paimon table from your data stream table
        DataType row = DataTypes.ROW(
                DataTypes.FIELD("pk", DataTypes.INT()),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("age", DataTypes.INT()),
                DataTypes.FIELD("dt", DataTypes.INT()));
        Schema schema = Schema.newBuilder().fromRowDataType(row).build();
        // Schema schema = Schema.newBuilder().build();

        Table tableSource = streamEnv.tableEnv.fromDataStream(source, schema);

        streamEnv.tableEnv.createTemporaryView("input_table", tableSource);

        // // insert into paimon table from your data stream table
        streamEnv.tableEnv
                .executeSql(String.format("INSERT INTO %s SELECT * FROM input_table", table))
                .await();
    }

    @Test
    public void batchWriteTo() throws ExecutionException, InterruptedException {
        BatchEnv batchEnv = new BatchEnv();
        // create paimon catalog
        batchEnv.tableEnv.executeSql(
                String.format(
                        "CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='file://%s/paimon_datalake')",
                        basePath));
        batchEnv.tableEnv.executeSql("USE CATALOG paimon");

        // register the table under a name and perform an aggregation
        batchEnv.tableEnv.executeSql(
                "CREATE TEMPORARY TABLE IF NOT EXISTS input_table (pk INT, name STRING, age INT, dt INT)"
                        + "WITH ("
                        + "'connector' = 'datagen',"
                        + "'rows-per-second'='10',"
                        + "'number-of-rows' = '500',"
                        + "'fields.name.length' = '10',"
                        + "'fields.pk.min' = '100001',"
                        + "'fields.pk.max' = '100021',"
                        + "'fields.dt.min' = '20230819',"
                        + "'fields.dt.max' = '20230820')");
        // create paimon table
        batchEnv.tableEnv.executeSql(
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s "
                                + "(pk INT, name STRING, age INT, dt INT, primary key(pk, dt) not enforced)"
                                + "partitioned by (dt) with (\n"
                                + "    'changelog-producer' = 'input',\n"
                                + "    'merge-engine'='deduplicate',\n"
                                + "    'bucket' = '1'\n"
                                + ")",
                        table));
        // insert into paimon table from your data stream table
        batchEnv.tableEnv
                .executeSql(String.format("INSERT INTO %s SELECT * FROM input_table", table))
                .await();
    }

    @Test
    public void streamReadFrom() throws Exception {
        // create environments of both APIs
        StreamEnv streamEnv = new StreamEnv(basePath);

        // create paimon catalog
        streamEnv.tableEnv.executeSql(
                String.format(
                        "CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='file://%s/paimon_datalake')",
                        basePath));
        streamEnv.tableEnv.executeSql("USE CATALOG paimon");

        streamEnv.tableEnv.executeSql(
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s "
                                + "(pk INT, name STRING, age INT, dt INT, primary key(pk, dt) not enforced)"
                                + "partitioned by (dt) with (\n"
                                + "    'changelog-producer' = 'input',\n"
                                + "    'merge-engine'='deduplicate',\n"
                                + "    'bucket' = '1'\n"
                                + ")",
                        table));

        // convert to DataStream
        TableResult tableResult = streamEnv.tableEnv.executeSql(String.format("SELECT * FROM %s ", table));

        tableResult.print();

        // DataStream<Row> dataStream = streamEnv.tableEnv.toChangelogStream(result);
        // use this datastream
        // dataStream.executeAndCollect().forEachRemaining(System.out::println);
        // prints:
        // +I[Bob, 12]
        // +I[Alice, 12]
        // -U[Alice, 12]
        // +U[Alice, 14]
    }

    @Test
    public void batchReadFrom() {
        // create environments of both APIs
        BatchEnv batchEnv = new BatchEnv();

        // create paimon catalog
        batchEnv.tableEnv.executeSql(
                String.format(
                        "CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='file://%s/paimon_datalake')",
                        basePath));
        batchEnv.tableEnv.executeSql("USE CATALOG paimon");

        batchEnv.tableEnv.executeSql(
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s "
                                + "(pk INT, name STRING, age INT, dt INT, primary key(pk, dt) not enforced)"
                                + "partitioned by (dt) with (\n"
                                + "    'changelog-producer' = 'input',\n"
                                + "    'merge-engine'='deduplicate',\n"
                                + "    'bucket' = '1'\n"
                                + ")",
                        table));

        // convert to DataStream
        Table result1 = batchEnv.tableEnv.sqlQuery(String.format("SELECT * FROM %s", table));
        result1.execute().print();

        // convert to DataStream
        Table result2 =
                batchEnv.tableEnv.sqlQuery(
                        String.format("SELECT COUNT(1) FROM %s WHERE dt = 20230820", table));

        result2.execute().print();
        // convert to DataStream
        Table result3 =
                batchEnv.tableEnv.sqlQuery(
                        String.format("SELECT COUNT(1) FROM %s WHERE dt = 20230819", table));
        result3.execute().print();
    }

    @Test
    public void batchChangeWriteToSamePartition() throws ExecutionException, InterruptedException {
        BatchEnv batchEnv = new BatchEnv();
        // create paimon catalog
        batchEnv.tableEnv.executeSql(
                String.format(
                        "CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='file://%s/paimon_datalake')",
                        basePath));
        batchEnv.tableEnv.executeSql("USE CATALOG paimon");

        // insert into paimon table from your data stream table
        // pk INT, name STRING, age INT, dt INT
        // 同分区更新
        batchEnv.tableEnv
                .fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("pk", DataTypes.INT()),
                                DataTypes.FIELD("name", DataTypes.STRING()),
                                DataTypes.FIELD("age", DataTypes.INT()),
                                DataTypes.FIELD("dt", DataTypes.INT())),
                        row(100001, "abc", 1, 20230820),
                        row(100001, "abc", 2, 20230819))
                .insertInto(table)
                .execute()
                .await();
    }

    @Test
    public void batchChangeWriteToPartitions() throws ExecutionException, InterruptedException {
        BatchEnv batchEnv = new BatchEnv();
        // create paimon catalog
        batchEnv.tableEnv.executeSql(
                String.format(
                        "CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='file://%s/paimon_datalake')",
                        basePath));
        batchEnv.tableEnv.executeSql("USE CATALOG paimon");

        // insert into paimon table from your data stream table
        // pk INT, name STRING, age INT, dt INT
        // 不同分区更新
        batchEnv.tableEnv
                .executeSql(String.format("UPDATE %s SET age = 222 WHERE pk = 100002", table))
                .await();
    }
}
