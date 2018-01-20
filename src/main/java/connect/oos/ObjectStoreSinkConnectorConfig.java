package connect.oos;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ObjectStoreSinkConnectorConfig extends AbstractConfig {

//    public static final String MY_SETTING_CONFIG = "my.setting";
//    private static final String MY_SETTING_DOC = "This is a setting important to my connector.";

    public ObjectStoreSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public ObjectStoreSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef();
                //.define(MY_SETTING_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, MY_SETTING_DOC);
    }

}
