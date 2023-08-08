package sourcecode.analysis;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/** */
public class BatchEnv {
    StreamExecutionEnvironment senv;
    TableEnvironment tableEnv;

    public BatchEnv() {
        this.senv = StreamExecutionEnvironment.createLocalEnvironment();
        int parallel = 1;
        senv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        senv.setParallelism(parallel);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();

        this.tableEnv = TableEnvironment.create(settings);
        this.tableEnv.getConfig().set(CoreOptions.DEFAULT_PARALLELISM, parallel);
    }
}
