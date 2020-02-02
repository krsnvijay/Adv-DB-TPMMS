import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

public class TPMMS {


  public void sortFile(String filePath) throws IOException {
    var tempFile = new File("Employee-Generator/temp-file.txt");
    var bufferedReader = new BufferedReader(new FileReader(filePath));
    var bufferedWriter = new BufferedWriter(new FileWriter(tempFile));
    var line = bufferedReader.readLine();
    var lines = new ArrayList<String>();
    while (line != null) {
      lines.add(line);
      if (lines.size() == 40) {
        // Block Limit reached
        // sort the block based on id , date
        Comparator<String> empIdComparator = Comparator
            .comparing((String record) -> Integer.parseInt(record.substring(0, 8)));
        lines.sort(empIdComparator.thenComparing(new RecordComparator()));
        //write sorted lines to temp file
        for (String ln : lines) {
          bufferedWriter.append(ln).append("\n");
        }
        // clear the block
        lines.clear();
      }
      line = bufferedReader.readLine();
    }
    bufferedReader.close();
    bufferedWriter.close();
    System.out.println("Sorted lines block wise!");
  }

}
