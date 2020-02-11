import java.io.IOException;
import java.io.RandomAccessFile;

public class MemoryHandler {
    public static final int FIVE_MB = 10;
    public static final int TEN_MB = 5;
    public static final int TWENTY_MB = 2;

    private static MemoryHandler instance = null;
    private static Runtime runtime = Runtime.getRuntime();

    public static MemoryHandler getInstance() {
        if (instance == null) instance = new MemoryHandler();
        return instance;
    }

    public long getFreeMemory() {
        return runtime.freeMemory();
    }

}
