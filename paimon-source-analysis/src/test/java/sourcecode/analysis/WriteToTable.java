package sourcecode.analysis;

import org.apache.flink.table.api.Table;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

/** */
public class WriteToTable {

    //
    private static final String basePath =
            "/Users/heap/Developer/code/incubator-paimon/paimon-source-analysis/src/test/resources";
    //
    private static final String table = "sink_paimon_table";

    @Test
    public void writeTo() throws ExecutionException, InterruptedException {
        StreamEnv streamEnv = new StreamEnv(basePath);
        // create paimon catalog
        streamEnv.tableEnv.executeSql(
                String.format(
                        "CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='file://%s/paimon_datalake')",
                        basePath));
        streamEnv.tableEnv.executeSql("USE CATALOG paimon");

        // register the table under a name and perform an aggregation
        streamEnv.tableEnv.executeSql(
                "CREATE TEMPORARY TABLE IF NOT EXISTS input_table (name STRING, age INT)"
                        + "WITH ('connector' = 'datagen','rows-per-second'='1','number-of-rows' = '300')");
        // create paimon table
        streamEnv.tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS sink_paimon_table (name STRING, age INT)");
        // insert into paimon table from your data stream table
        streamEnv
                .tableEnv
                .executeSql(String.format("INSERT INTO %s SELECT * FROM input_table", table))
                .await();
    }

    @Test
    public void streamReadFrom() {
        // create environments of both APIs
        StreamEnv streamEnv = new StreamEnv(basePath);

        // create paimon catalog
        streamEnv.tableEnv.executeSql(
                String.format(
                        "CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='file://%s/paimon_datalake')",
                        basePath));
        streamEnv.tableEnv.executeSql("USE CATALOG paimon");

        // convert to DataStream
        Table result = streamEnv.tableEnv.sqlQuery(String.format("SELECT * FROM %s", table));

        result.execute();

        // DataStream<Row> dataStream = tableEnv.toChangelogStream(table);
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

        // convert to DataStream
        Table result = batchEnv.tableEnv.sqlQuery(String.format("SELECT * FROM %s", table));

        result.execute();

        // DataStream<Row> dataStream = tableEnv.toChangelogStream(table);
        // use this datastream
        // dataStream.executeAndCollect().forEachRemaining(System.out::println);

        // prints:
        // +I[Bob, 12]
        // +I[Alice, 12]
        // -U[Alice, 12]
        // +U[Alice, 14]
    }
}
