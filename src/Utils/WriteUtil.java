package Utils;

import Models.Employee;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

public class WriteUtil extends BufferedWriter {
    public boolean freshBlock = true;
    public int IOOperations = 0;

    public WriteUtil(File file) throws IOException {
        super(new java.io.FileWriter(file));
    }

    public String getTupleString(Employee tuple) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return String.format("%1$8d", tuple.empID) +
                df.format(tuple.lastUpdated) +
                String.format("%1$-25s", tuple.empName) +
                String.format("%1$1d", tuple.gender) +
                String.format("%03d", tuple.dept) +
                String.format("%1$-9s", tuple.sin) +
                String.format("%1$-43s", tuple.address);
    }

    public void writeChunk(List<Employee> chunk, Byte tuplesInChunk) {
        try {
            if (chunk != null) {
                if (chunk.size() % tuplesInChunk == 0)
                    IOOperations += Math.floorDiv(chunk.size(), tuplesInChunk);
                else
                    IOOperations += Math.floorDiv(chunk.size(), tuplesInChunk) + 1;
                for (Employee tuple : chunk) {
                    if (freshBlock) {
                        this.write(getTupleString(tuple));
                        freshBlock = false;
                    } else {
                        this.newLine();
                        this.write(getTupleString(tuple));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
