import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

public class FileWriter extends BufferedWriter {
    public int ioCounter = 0;
    public boolean isNewBatch = true;

    public FileWriter(File file) throws IOException {
        super(new java.io.FileWriter(file));
    }

    //write one batch back to file
    public void writeOneBatch(List<Tuple> oneBatch, Byte tupleNumInOneBlock) {
        try {
            if (oneBatch != null) {
                if (oneBatch.size() % tupleNumInOneBlock == 0)
                    ioCounter += Math.floorDiv(oneBatch.size(), tupleNumInOneBlock);
                else
                    ioCounter += Math.floorDiv(oneBatch.size(), tupleNumInOneBlock) + 1;
                for (Tuple tuple : oneBatch) {
                    if (isNewBatch) {
                        this.write(tupleParser(tuple));
                        isNewBatch = false;
                    } else {
                        this.newLine();
                        this.write(tupleParser(tuple));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String tupleParser(Tuple tuple) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-mm-dd");
        return String.format("%1$8d", tuple.empID) +
                dateFormatter.format(tuple.lastUpdated) +
                String.format("%1$-25s", tuple.empName) +
                String.format("%1$1d", tuple.gender) +
                String.format("%1$3d", tuple.dept) +
                String.format("%1$-9s", tuple.sin) +
                String.format("%1$-43s", tuple.address);
    }

}
