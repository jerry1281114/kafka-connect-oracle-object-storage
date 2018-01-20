package connect.oos;

import connect.oos.dummy.DummyRecordWriter;
import connect.oos.util.FileUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

public class TopicPartitionWriter {
    private static final Logger log = LoggerFactory.getLogger(TopicPartitionWriter.class);
    private static final long BUFFER_LIMIT = 10;

    private final TopicPartition tp;
    private final ObjectStorageService objectStorageService;
    private final SinkTaskContext sinkTaskContext;
    private long currentOffSet = -1L;
    private Queue<SinkRecord> buffer = new LinkedList<>();
    private RecordWriter<SinkRecord> recordWriter;

    public TopicPartitionWriter(TopicPartition tp,
                                ObjectStorageService objectStorageService,
                                SinkTaskContext sinkTaskContext) {
        log.info("tpw.init initialising TopicPartitionWriter for {}", tp);
        this.tp = tp;
        this.objectStorageService = objectStorageService;
        this.sinkTaskContext = sinkTaskContext;
        objectStorageService.mkDirs(FileUtils.getTempTopicPartitionDir(tp));
        objectStorageService.mkDirs(FileUtils.getObjectStorageTempTopicPartitionDir(tp));
        objectStorageService.mkDirs(FileUtils.getObjectStorageTopicPartitionDir(tp));
        recover();
    }

    public void close() {
        log.info("tpw.close");
        //close temp
        //delete temp
        //clear out instance variables
    }

    public void write(SinkRecord record) {
        buffer.add(record);
        if(shouldSyncBuffers()) {
            try{
                fsync();
            } catch (Exception e) {
                log.error("tpw.write failed to write", e);
                assertCondition(false, "Failed to write file {}", e.getMessage());
            }
        }
    }

    private boolean shouldSyncBuffers() {
        log.info("tpw.shouldSyncBuffers checking if buffers need syncing for {}", tp);
        return buffer.size() >= BUFFER_LIMIT;
    }

    public void fsync() throws IOException {
        log.info("tpw.fsync syncing changes from buffer to file for {}. Pausing incoming records", tp);
        sinkTaskContext.pause(tp);
        if(!buffer.isEmpty()) {
            int bufferSize = buffer.size();
            Long beginOffset = currentOffSet;
            Long endOffset = currentOffSet + bufferSize -1;
            assertCondition(beginOffset >= 0, "beginOffset {} is not >= 0", beginOffset);
            assertCondition(endOffset > 0, "endOffset {} is not > 0", endOffset);
            Long recordCounter = 0L;
            String tempLocalFilePath = FileUtils.getTempFilePath(tp, beginOffset, endOffset);
            String tempCasperFilePath = FileUtils.getObjectStorageTempFilePath(tp, beginOffset, endOffset);
            String finalCasperFilePath = FileUtils.getObjectStorageFilePath(tp, beginOffset, endOffset);
            recordWriter = new DummyRecordWriter(tp, tempLocalFilePath);
            log.info("tpw.fsync buffer.size={} offset={}, recordCounter={}", buffer.size(), currentOffSet, recordCounter);
            while(!buffer.isEmpty()) {
                SinkRecord record = buffer.poll();
                Long expectedOffset = currentOffSet +recordCounter;
                assertCondition(record.kafkaOffset() == expectedOffset, "offset in kafka {} does not match expected {}", record.kafkaOffset(), expectedOffset);
                recordWriter.write(record);
                recordCounter++;
            }
            recordWriter.close();
            objectStorageService.upload(tempLocalFilePath, tempCasperFilePath);
            objectStorageService.delete(tempLocalFilePath);
            objectStorageService.rename(tempCasperFilePath, finalCasperFilePath);
            objectStorageService.delete(tempCasperFilePath);
            currentOffSet = currentOffSet + recordCounter;
            log.info("tpw.fsync buffer.size={} offset={} recordCounter={} beginOffset={} endOffset={}", buffer.size(), currentOffSet, recordCounter, beginOffset, endOffset);
            log.info("tpw.fsync resetting recordCounter and resuming incoming records");
        }
        sinkTaskContext.resume(tp);
    }

    private void recover() {
        log.info("tpw.recover recovering for {}. Pausing incoming records", tp);
        sinkTaskContext.pause(tp);
        Long lastOffset = objectStorageService.getLastOffset(FileUtils.getObjectStorageTopicPartitionDir(tp));
        currentOffSet = lastOffset == 0 ? lastOffset: lastOffset+1;
        log.info("tpw.recover received last committed offset for {} from object storage service as {}", tp, currentOffSet);
        log.info("tpw.recover seeking {} to offset {} and resuming incoming records", tp, currentOffSet);
        sinkTaskContext.offset(tp, currentOffSet);
        sinkTaskContext.resume(tp);
    }

    private void assertCondition(boolean condition, String msg, Object ... args) {
        if(!condition) {
            log.info("tpw.assertCondition "+msg, args);
            throw new RuntimeException("assert failed");
        }
    }
}
