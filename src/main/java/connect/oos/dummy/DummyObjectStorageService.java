package connect.oos.dummy;

import connect.oos.ObjectStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;

public class DummyObjectStorageService implements ObjectStorageService {
    private static final Logger log = LoggerFactory.getLogger(DummyObjectStorageService.class);

    @Override
    public boolean mkDirs() {
        log.info("creating directory");
        return false;
    }

    @Override
    public OutputStream create() {
        log.info("creating file");
        return null;
    }

    @Override
    public boolean rename() {
        log.info("renaming file");
        return false;
    }

    @Override
    public boolean delete() {
        log.info("deleting file");
        return false;
    }

    @Override
    public long getLastOffset() {
        log.info("fetching last offset");
        return 0;
    }
}
