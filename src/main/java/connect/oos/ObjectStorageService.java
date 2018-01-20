package connect.oos;

import java.io.OutputStream;

public interface ObjectStorageService {
    boolean mkDirs();
    OutputStream create();
    boolean rename();
    boolean delete();
    long getLastOffset();
}
