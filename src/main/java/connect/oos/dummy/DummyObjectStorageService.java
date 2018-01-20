package connect.oos.dummy;

import connect.oos.ObjectStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class DummyObjectStorageService implements ObjectStorageService {
    private static final Logger log = LoggerFactory.getLogger(DummyObjectStorageService.class);

    @Override
    public boolean mkDirs(String path) {
        log.info("objectStorageService.mkDirs {}", path);
        File dir = new File(path);
        if(!dir.exists()) {
            return dir.mkdirs();
        }
        return true;
    }

    @Override
    public boolean upload(String localPath, String storageKey) {
        log.info("objectStorageService.upload {} -> {}", localPath, storageKey);
        File sourceFile = new File(localPath);
        File targetFile = new File(storageKey);
        if(sourceFile.exists()) {
            return sourceFile.renameTo(targetFile);
        }
        return false;
    }

    @Override
    public boolean rename(String oldKey, String newKey) {
        log.info("objectStorageService.rename {} -> {}", oldKey, newKey);
        File sourceFile = new File(oldKey);
        File targetFile = new File(newKey);
        if(sourceFile.exists()) {
            return sourceFile.renameTo(targetFile);
        }
        return false;
    }

    @Override
    public boolean delete(String storageKey) {
        log.info("objectStorageService.delete {}", storageKey);
        File f = new File(storageKey);
        if(f.exists()) {
            return f.delete();
        }
        return false;
    }

    @Override
    public long getLastOffset(String dirPath) {
        log.info("objectStorageService.getLastOffset {}", dirPath);
        File dir = new File(dirPath);
        Long maxOffset = 0L;
        if(dir.exists()) {
            File[] files = dir.listFiles();
            for(File f : files) {
                if(f.isDirectory()) {
                    continue;
                }
                String[] splits = f.getName().split("-");
                Long offset = Long.valueOf(splits[splits.length-1]);
                if(offset > maxOffset) {
                    maxOffset = offset;
                }
            }
        }
        return maxOffset;
    }
}
