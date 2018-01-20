package connect.oos.dummy;

import connect.oos.RecordWriter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class DummyRecordWriter implements RecordWriter<SinkRecord> {
    private static final Logger log = LoggerFactory.getLogger(DummyRecordWriter.class);

    private final TopicPartition tp;
    private final String filePath;
    private PrintWriter printWriter;


    public DummyRecordWriter(TopicPartition tp, String filePath) throws IOException {
        this.tp = tp;
        this.filePath = filePath;
        FileWriter fileWriter = new FileWriter(filePath, false);
        printWriter = new PrintWriter(fileWriter);
    }

    @Override
    public void write(SinkRecord value) {
        log.info("writing record with offset={} for tp={} in file={}", value.kafkaOffset(), tp, filePath);
        printWriter.print(value);
    }

    @Override
    public void close() {
        log.info("closing record writer stream of file={} for tp={}", filePath, tp);
        printWriter.close();
    }
}
