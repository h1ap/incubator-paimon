package sourcecode.analysis;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.paimon.disk.ExternalBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 *
 */
public class StreamEnv {

    private static final Logger LOG = LoggerFactory.getLogger(StreamEnv.class);
    StreamExecutionEnvironment senv;
    StreamTableEnvironment tableEnv;

    public StreamEnv(String path) {
        Configuration cfg = new Configuration();
        int port = RandomUtils.nextInt(8000, 8999);
        LOG.info("current flink cluster port: {}", port);
        cfg.set(RestOptions.PORT, port);
        this.senv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg);
        int parallel = 1;

        int cp = 60;

        senv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        senv.setParallelism(parallel);
        senv.getCheckpointConfig().setCheckpointStorage("file://" + path + "/flink-state");
        senv.enableCheckpointing(cp * 1000, CheckpointingMode.EXACTLY_ONCE);
        EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
        stateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        senv.setStateBackend(stateBackend);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        // env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        this.tableEnv = StreamTableEnvironment.create(senv, settings);

        this.tableEnv.getConfig()
                .getConfiguration()
                .set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
                .set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(cp))
                .set(CoreOptions.DEFAULT_PARALLELISM, parallel);
    }
}
