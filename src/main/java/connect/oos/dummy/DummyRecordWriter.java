package connect.oos.dummy;

import connect.oos.RecordWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyRecordWriter implements RecordWriter<SinkRecord> {
    private static final Logger log = LoggerFactory.getLogger(DummyRecordWriter.class);

    @Override
    public void write(SinkRecord value) {
        log.info("writing record {}", value);
    }

    @Override
    public void close() {
        log.info("closing record writer stream");
    }
}
