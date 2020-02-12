import com.sun.prism.shader.AlphaOne_RadialGradient_AlphaTest_Loader;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class TPMMS {


  public void sortFile(String filePath) throws IOException {
    File tempFile = new File("Employee-Generator/temp-file.txt");
    BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
    BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tempFile));
    String line = bufferedReader.readLine();
    ArrayList<String> lines = new ArrayList<String>();

    int numOfRecords = (int) Files.lines(Paths.get(filePath)).count();
    int pageSize = (int) Math.ceil((numOfRecords*100f)/Runtime.getRuntime().freeMemory()) + 1;
    int lastFew = numOfRecords % pageSize;

    int passes = 0;
    while (line != null) {
      lines.add(line);
      if (lines.size() == 6250) {
        // Block Limit reached
        // sort the block based on id , date
        writeToFile(bufferedWriter, lines);
        passes++;
      }
      line = bufferedReader.readLine();
    }

    writeToFile(bufferedWriter, lines);
    passes++;
    bufferedReader.close();
    bufferedWriter.close();
    mergeBlocks(tempFile, numOfRecords, passes);
    System.out.println("Sorted lines block wise!");
  }

  private void writeToFile(BufferedWriter bufferedWriter, ArrayList<String> lines) throws IOException {
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

  public void mergeBlocks(File tempFilePath, int numOfRecords, int k) throws IOException {
    File finalFile = new File("Employee-Generator/sorted.txt");
    RandomAccessFile raf = new RandomAccessFile(tempFilePath, "r");
    BufferedWriter writer = new BufferedWriter(new FileWriter(finalFile));

    int[] runPointers = new int[k-1];
    long bufferSize = Runtime.getRuntime().freeMemory()/k;
    int tuplesPerBlock = (int) bufferSize/100;
    ArrayList<String> lines = new ArrayList<>();

    while(!exhaustedBlocks(runPointers, numOfRecords)) {
      int minIndex = 0;
      for (int i = 0; i < k; i++) {
        raf.seek(runPointers[i]*bufferSize);
        // Read the lines
        for(int j=0; j<tuplesPerBlock; j++) {
          lines.add(raf.readLine());
        }
      }

      // Sort the lines
      for(int i=0; i<k; i++) {
        lines.get(runPointers[i]*tuplesPerBlock);
      }

    }
  }

  private boolean exhaustedBlocks(int[] runsPerPagePointers, int maxTuples) {
    int sum = 0;
    for(int pointer:runsPerPagePointers){
      sum += pointer;
    }
    return sum == maxTuples;
  }

}
