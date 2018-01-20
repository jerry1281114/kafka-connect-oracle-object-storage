package connect.oos.util;

import org.apache.kafka.common.TopicPartition;

public class FileUtils {

    private static final String LOCAL_PREFIX = "/tmp/oos";

    public static String getTempFilePath(TopicPartition tp, Long beginOffset, Long endOffset) {
            return String.format(LOCAL_PREFIX + "/tmp/%s/%s/%d-%d", tp.topic(), tp.partition(), beginOffset, endOffset);
    }

    public static String getObjectStorageTempFilePath(TopicPartition tp, Long beginOffset, Long endOffset) {
        return String.format(LOCAL_PREFIX + "/casper/+tmp/%s/%s/%d-%d", tp.topic(), tp.partition(), beginOffset, endOffset);
    }

    public static String getObjectStorageFilePath(TopicPartition tp, Long beginOffset, Long endOffset) {
        return String.format(LOCAL_PREFIX + "/casper/%s/%s/%d-%d", tp.topic(), tp.partition(), beginOffset, endOffset);
    }

    public static String getTempTopicPartitionDir(TopicPartition tp) {
        return String.format(LOCAL_PREFIX + "/tmp/%s/%s", tp.topic(), tp.partition());
    }

    public static String getObjectStorageTempTopicPartitionDir(TopicPartition tp) {
        return String.format(LOCAL_PREFIX + "/casper/+tmp/%s/%s", tp.topic(), tp.partition());
    }

    public static String getObjectStorageTopicPartitionDir(TopicPartition tp) {
        return String.format(LOCAL_PREFIX + "/casper/%s/%s", tp.topic(), tp.partition());
    }

}
