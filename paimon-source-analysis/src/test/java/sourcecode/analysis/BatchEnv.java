package sourcecode.analysis;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class BatchEnv {

    private static final Logger LOG = LoggerFactory.getLogger(BatchEnv.class);
    StreamExecutionEnvironment senv;
    TableEnvironment tableEnv;

    public BatchEnv() {
        Configuration cfg = new Configuration();
        int port = RandomUtils.nextInt(8000, 8999);
        LOG.info("current flink cluster port: {}", port);
        cfg.set(RestOptions.PORT, port);
        this.senv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(cfg);
        int parallel = 1;
        senv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        senv.setParallelism(parallel);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();

        this.tableEnv = TableEnvironment.create(settings);
        this.tableEnv.getConfig().set(CoreOptions.DEFAULT_PARALLELISM, parallel);
    }
}
