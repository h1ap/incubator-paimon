package sourcecode.analysis;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** */
public class StreamEnv {
    StreamExecutionEnvironment senv;
    StreamTableEnvironment tableEnv;

    public StreamEnv(String path) {
        this.senv = StreamExecutionEnvironment.createLocalEnvironment();
        int parallel = 1;
        senv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        senv.setParallelism(parallel);
        senv.getCheckpointConfig().setCheckpointStorage("file://" + path + "/flink-state");
        senv.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
        stateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        senv.setStateBackend(stateBackend);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        // env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        this.tableEnv = StreamTableEnvironment.create(senv, settings);
    }
}
