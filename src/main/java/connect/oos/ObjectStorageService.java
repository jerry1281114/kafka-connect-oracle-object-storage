package connect.oos;

public interface ObjectStorageService {
    boolean mkDirs(String path);
    boolean upload(String localPath, String storageKey);
    boolean rename(String oldKey, String newKey);
    boolean delete(String storageKey);
    long getLastOffset(String dirPath);
}
