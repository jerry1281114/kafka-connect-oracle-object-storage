package connect.oos.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionUtil {
    private static final Logger log = LoggerFactory.getLogger(VersionUtil.class);
    private static final String DEFAULT_VERSION = "0.0.0.0";
    private static String version = DEFAULT_VERSION;
    static {
        try {
            String implementationVersion = VersionUtil.class.getPackage().getImplementationVersion();
            if(implementationVersion != null) {
                version = implementationVersion;
            }
            log.info("version={}", version);
        } catch(Exception ex){
            log.warn("Error while trying to get implementation version", ex);
        }
    }

    public static String getVersion() {
        return version;
    }
}