import java.io.IOException;
import java.io.RandomAccessFile;

public class MemoryHandler {
    long totalMemory;
    long usedMemory;

    private static MemoryHandler instance = null;

    public static MemoryHandler getInstance(int totalInMb) {
        if (instance == null) instance = new MemoryHandler(totalInMb);
        return instance;
    }

    public static MemoryHandler getInstance() {
        if (instance == null) instance = new MemoryHandler();
        return instance;
    }

    public MemoryHandler() {
        this.totalMemory = 5 * 1024;
        this.usedMemory = 0;
    }

    public MemoryHandler(long totalInMb) {
        this.totalMemory = totalInMb * 1024;
        this.usedMemory = 0;
    }

    public long getFreeMemory() {
        return totalMemory - usedMemory;
    }

    public long getTotalMemory() {
        return totalMemory;
    }

}
