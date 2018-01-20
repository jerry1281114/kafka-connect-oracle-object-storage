package connect.oos;

import connect.oos.dummy.DummyObjectStorageService;
import connect.oos.util.VersionUtil;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ObjectStoreSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(ObjectStoreSinkTask.class);

    private Collection<TopicPartition> assignment = new HashSet<>();
    private Map<TopicPartition, TopicPartitionWriter> topicPartitionWriters = new HashMap<>();
    private ObjectStorageService objectStorageService = new DummyObjectStorageService();

    @Override
    public void start(Map<String, String> props) {
        log.info("sinkTask.start");
        MDC.put("wid", props.get(Constants.WORKER_ID));
        props.keySet().forEach(key -> log.info("sinkTask.start.props[{}]->{}", key, props.get(key)));
    }

    @Override
    public void stop() {
        log.info("sinkTask.stop");
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        log.info("sinkTask.open");
        this.assignment = partitions;
        assignment.forEach(tp -> {
            log.info("sinkTask.open.partitions: {}", tp);
            topicPartitionWriters.put(tp, new TopicPartitionWriter(tp, objectStorageService, this.context));
        });
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        log.info("sinkTask.close");
        partitions.forEach(tp-> {
            log.info("sinkTask.close.partitions: {}", tp);
            TopicPartitionWriter tpw = topicPartitionWriters.get(tp);
            if(tpw == null) {
                log.info("sinkTask.close contained {} in topic-partitions where as the writers map does not contain any writer for it", tpw);
            } else {
                tpw.close();
            }
        });
        topicPartitionWriters.clear();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("sinkTask.put size={}", records.size());
        records.forEach(record -> {
            log.info("sinkTask.put tp={}-{} offset={} key={} value={}", record.topic(), record.kafkaPartition(), record.kafkaOffset(), record.key(), record.value());
            TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            TopicPartitionWriter tpw = topicPartitionWriters.get(tp);
            if(tpw == null) {
                log.info("sinkTask.put records contained {} where as the writers map does not contain any writer for it", tp);
            } else {
                tpw.write(record);
            }
        });
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        log.info("sinkTask.flush");
        currentOffsets.keySet().forEach(tp -> log.info("sinkTask.flush.currentOffsets[{}]->{}", tp, currentOffsets.get(tp)));
    }

    @Override
    public String version() {
        log.info("sinkTask.version");
        return VersionUtil.getVersion();
    }
}
