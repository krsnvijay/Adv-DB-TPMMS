import java.io.IOException;
import java.io.RandomAccessFile;

public class MemoryHandler {

    private static MemoryHandler instance = null;
    private static Runtime runtime = Runtime.getRuntime();

//    public static MemoryHandler getInstance(int totalInMb) {
//        if (instance == null) instance = new MemoryHandler(totalInMb);
//        return instance;
//    }

    public static MemoryHandler getInstance() {
        if (instance == null) instance = new MemoryHandler();
        return instance;
    }

//    public MemoryHandler(long totalInMb) {
//        this.totalMemory = totalInMb * 1024;
//        this.usedMemory = 0;
//    }

    public long getFreeMemory() {
        return runtime.freeMemory();
    }

    public long getTotalMemory() {
        return runtime.totalMemory();
    }

}
