package connect.oos;

public interface RecordWriter<T> {
    void write(T value);
    void close();
}
