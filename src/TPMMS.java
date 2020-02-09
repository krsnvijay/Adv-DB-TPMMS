import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

public class TPMMS {
  private final double SIZE_OF_RECORD = 101.0;
  private static BufferedReader reader;
  private static BufferedWriter writer;
  private static StringBuilder opString = new StringBuilder();

  private File tempFile = new File("Employee-Generator/temp-file.txt");

  public void sortFile(String filePath) throws IOException {
    reader = new BufferedReader(new FileReader(filePath));
    writer = new BufferedWriter(new FileWriter(tempFile));

    File inpFile = new File(filePath);
    long fileSize = inpFile.length();
    double numOfRecords = fileSize/SIZE_OF_RECORD;

    long totalNumOfPages = (long) Math.floor(MemoryHandler.getInstance().getFreeMemory()/numOfRecords);
    int numOfTuplesPerPage = (int) Math.floor(numOfRecords/(totalNumOfPages*5f));

    //String line = reader.readLine();
    ArrayList<String> lines = new ArrayList<>();

    System.out.println(fileSize+" "+numOfRecords+" "+totalNumOfPages+" "+numOfTuplesPerPage);
    //numOfTuplesPerPage = 40;
    while (reader.readLine() != null) {
      lines.add(reader.readLine());

      if (lines.size() == numOfTuplesPerPage) {
        // Block Limit reached
        // sort the block based on id , date
        Comparator<String> empIdComparator = Comparator
            .comparing((String record) -> Integer.parseInt(record.substring(0, 8)));
        lines.sort(empIdComparator.thenComparing(new RecordComparator()));

        //write sorted lines to temp file
        for (String ln : lines) {
          opString.append(ln).append("\n");
        }

        // clear the block
        lines.clear();
        writer.append(opString);
        opString.delete(0, opString.length());
      }
//      System.out.println("Free memory: " + MemoryHandler.getInstance().getFreeMemory());
      //line = reader.readLine();
    }
    reader.close();
    writer.close();
    System.out.println("Sorted lines block wise!");
  }

}
