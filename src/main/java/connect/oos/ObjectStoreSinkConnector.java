package connect.oos;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObjectStoreSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(ObjectStoreSinkConnector.class);
    private ObjectStoreSinkConnectorConfig config;

    @Override
    public void start(Map<String, String> props) {
        log.info("sinkConnector.start");
        props.keySet().forEach(key -> log.info("sinkConnector.start.props[{}]->{}", key, props.get(key)));
        config = new ObjectStoreSinkConnectorConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        log.info("sinkConnector.taskClass");
        return ObjectStoreSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("sinkConnector.taskConfigs maxTasks={}", maxTasks);
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskProps = new HashMap<>();
            taskProps.put(Constants.WORKER_ID, Integer.toString(i));
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("sinkConnector.stop");
    }

    @Override
    public ConfigDef config() {
        log.info("sinkConnector.config");
        return ObjectStoreSinkConnectorConfig.conf();
    }

    @Override
    public String version() {
        log.info("sinkConnector.version");
        return VersionUtil.getVersion();
    }
}
