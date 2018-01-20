package connect.oos;

import connect.oos.dummy.DummyRecordWriter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

public class TopicPartitionWriter {
    private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriter.class);

    private final TopicPartition tp;
    private final ObjectStorageService objectStorageService;
    private final SinkTaskContext sinkTaskContext;
    private long offSet = -1L;
    private long recordCounter = 0L;
    private Queue<SinkRecord> buffer = new LinkedList<>();
    private RecordWriter<SinkRecord> recordWriter;

    public TopicPartitionWriter(TopicPartition tp,
                                ObjectStorageService objectStorageService,
                                SinkTaskContext sinkTaskContext) {
        log.info("Initialising TopicPartitionWriter for {}", tp);
        this.tp = tp;
        this.objectStorageService = objectStorageService;
        this.sinkTaskContext = sinkTaskContext;
        recover();
    }

    public void write(SinkRecord record) {
        buffer.add(record);
        if(shouldRotateFile()) {
            fsync();
        }
    }

    public void close() {
        //close temp
        //delete temp
        //clear out instance variables
    }

    public void fsync() {
        log.info("Syncing changes from buffer to file for {}", tp);
        pauseConsumption();
        recordWriter = new DummyRecordWriter();
        while(!buffer.isEmpty()) {
            SinkRecord record = buffer.poll();
            recordWriter.write(record);
            recordCounter++;
        }
        recordWriter.close();
        offSet = offSet + recordCounter;
        resumeConsumption();
    }

    private boolean shouldRotateFile() {
        log.info("checking if file needs rotation for {}", tp);
        return new Random().nextBoolean();
    }

    private void pauseConsumption() {
        log.info("Pausing record consumption for {}", tp);
        sinkTaskContext.pause(tp);
    }

    private void resumeConsumption() {
        log.info("Resuming record consumption for {}", tp);
        sinkTaskContext.resume(tp);
    }

    private void recover() {
        log.info("Recovering TopicPartitionWriter for {}", tp);
        pauseConsumption();
        offSet = objectStorageService.getLastOffset();
        log.info("Received last committed offset for {} from object storage service as {}", tp, offSet);
        log.info("Seeking {} to offset {}", tp, offSet);
        sinkTaskContext.offset(tp, offSet);
        resumeConsumption();
    }
}
