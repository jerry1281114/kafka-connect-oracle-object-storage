package connect.oos;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ObjectStoreSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(ObjectStoreSinkTask.class);

    @Override
    public void initialize(SinkTaskContext context) {
        super.initialize(context);
        log.info("sinkTask.initialize");
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        log.info("sinkTask.flush");
        currentOffsets.keySet().forEach(tp -> log.info("sinkTask.flush.currentOffsets[{}]->{}", tp, currentOffsets.get(tp)));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        log.info("sinkTask.preCommit");
        currentOffsets.keySet().forEach(tp -> log.info("sinkTask.flush.preCommit[{}]->{}", tp, currentOffsets.get(tp)));
        return new HashMap<>();
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        log.info("sinkTask.open");
        partitions.forEach(tp-> log.info("sinkTask.open.partitions: {}", tp));
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        log.info("sinkTask.close");
        partitions.forEach(tp-> log.info("sinkTask.close.partitions: {}", tp));
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("sinkTask.start");
        MDC.put("wid", props.get(Constants.WORKER_ID));
        props.keySet().forEach(key -> log.info("sinkTask.start.props[{}]->{}", key, props.get(key)));
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("sinkTask.put size={}", records.size());
        records.forEach(record -> log.info("sinkTask.put tp={}-{} offset={} key={} value={}", record.topic(), record.kafkaPartition(), record.kafkaOffset(), record.key(), record.value()));
    }

    @Override
    public void stop() {
        log.info("sinkTask.stop");
    }

    @Override
    public String version() {
        log.info("sinkTask.version");
        return VersionUtil.getVersion();
    }
}
