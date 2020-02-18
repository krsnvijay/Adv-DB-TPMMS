package commonutils;

import models.Employee;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ReadUtil extends BufferedReader  {

    public boolean done;
    public File file;
    public short tuplesPerChunk = 40;
    public int IOOperations;

    public ReadUtil(File file) throws FileNotFoundException {
        super(new java.io.FileReader(file));
        this.file = file;
    }

    private Employee serialize(String line) {
        try {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            int empId = Integer.parseInt(line.substring(0, 8));
            Date lastUpdated = df.parse(line.substring(8, 18));
            String empName = line.substring(18, 43).trim();
            short gender = Short.parseShort(line.substring(43, 44));
            short dept = Short.parseShort(line.substring(44, 47));
            String sin = line.substring(47, 56).trim();
            String address = line.substring(56, 99).trim();
            return new Employee(empId, lastUpdated, empName, gender, dept, sin, address);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<Employee> readChunk() {
        List<Employee> chunk = new ArrayList<>(tuplesPerChunk);
        if (Runtime.getRuntime().freeMemory() > Runtime.getRuntime().maxMemory()*0.15f) {
            for (int i = 0; i < tuplesPerChunk; i++) {
                Employee tuple = readTuple();
                if (tuple == null)
                    break;
                chunk.add(tuple);
            }
            if (chunk.size() != 0)
                IOOperations++;
        }
        return chunk;
    }

    private Employee readTuple() {
        try {
            String nextLine = this.readLine();
            if (nextLine == null || nextLine.trim().equals("")) {
                done = true;
                return null;
            }
            return serialize(nextLine);
        } catch (IOException e) {
            e.printStackTrace();
            done = true;
            return null;
        }
    }

}
