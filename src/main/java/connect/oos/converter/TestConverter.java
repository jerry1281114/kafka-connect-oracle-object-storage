package connect.oos.converter;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TestConverter implements Converter {

    private static final Logger log = LoggerFactory.getLogger(TestConverter.class);


    private final StringSerializer serializer = new StringSerializer();
    private final StringDeserializer deserializer = new StringDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.info("testConverter.configure isKey={}", isKey);
        configs.keySet().forEach(key -> {
            log.info("testConverter.configure[{}]={}", key, configs.get(key));
        });
        Map<String, Object> serializerConfigs = new HashMap<>();
        serializerConfigs.putAll(configs);
        Map<String, Object> deserializerConfigs = new HashMap<>();
        deserializerConfigs.putAll(configs);

        Object encodingValue = configs.get("converter.encoding");
        if (encodingValue != null) {
            serializerConfigs.put("serializer.encoding", encodingValue);
            deserializerConfigs.put("deserializer.encoding", encodingValue);
        }

        serializer.configure(serializerConfigs, isKey);
        deserializer.configure(deserializerConfigs, isKey);
    }

    /**
     * Convert a Kafka Connect data object to a native object for serialization.
     *
     * @param topic  the topic associated with the data
     * @param schema the schema for the value
     * @param value  the value to convert
     * @return the serialized value
     */
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            log.info("testConverter.fromConnectData topic={}, schema={}, value={}", topic, schema, value);
            return serializer.serialize(topic, value == null ? null : value.toString());
        } catch (SerializationException e) {
            throw new DataException("Failed to serialize to a string: ", e);
        }
    }

    /**
     * Convert a native object to a Kafka Connect data object.
     *
     * @param topic the topic associated with the data
     * @param value the value to convert
     * @return an object containing the {@link Schema} and the converted value
     */
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            log.info("testConverter.toConnectData topic={} byte[].size={}", topic, value == null ? 0 : value.length);
            return new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, deserializer.deserialize(topic, value));
        } catch (SerializationException e) {
            throw new DataException("Failed to deserialize string: ", e);
        }
    }
}
