import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FileReader extends BufferedReader {

    public float preserveMemory;
    public File file;
    public short tupleNumInOneBlock = 15;
    public int ioCounter;
    public boolean finish;

    public FileReader(File file, float preserveMemPercentage) throws FileNotFoundException {
        super(new java.io.FileReader(file));
        this.preserveMemory = Runtime.getRuntime().maxMemory() * preserveMemPercentage;
        this.file = file;
    }

    public List<Tuple> getOneBlock() {
        List<Tuple> oneBlock = new ArrayList<>(tupleNumInOneBlock);
        if (Runtime.getRuntime().freeMemory() > preserveMemory) {
            for (int i = 0; i < tupleNumInOneBlock; i++) {
                Tuple oneTuple = getOneTuple();
                if (oneTuple == null)
                    break;
                oneBlock.add(oneTuple);
            }
            if (oneBlock.size() != 0)
                ioCounter++;
        }
        return oneBlock;
    }

    private Tuple getOneTuple() {
        try {
            String nextLine = this.readLine();
            if (nextLine == null || nextLine.trim().equals("")) {
                finish = true;
                return null;
            }
            return stringParser(nextLine);
        } catch (IOException e) {
            e.printStackTrace();
            finish = true;
            return null;
        }
    }

    //parse one line file string to one tuple object
    private Tuple stringParser(String line) {
        try {
            SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-mm-dd");
            int empId = Integer.parseInt(line.substring(0, 8));
            Date lastUpdated = dateFormatter.parse(line.substring(8, 18));
            String empName = line.substring(18, 43).trim();
            short gender = Short.parseShort(line.substring(43, 44));
            short dept = Short.parseShort(line.substring(44, 47));
            String sin = line.substring(47, 56).trim();
            String address = line.substring(56, 99).trim();
            return new Tuple(empId, lastUpdated, empName, gender, dept, sin, address);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

}
