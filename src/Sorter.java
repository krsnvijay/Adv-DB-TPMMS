import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;

public class Sorter {

  final int TUPLE_SIZE = 101;
  final int BLOCK_SIZE = 5000;
  final long AVAILABLE_MEMORY;
  final String path = "Employee-Generator/out/tuples";

  public Sorter() {
    AVAILABLE_MEMORY = Runtime.getRuntime().freeMemory();
  }


  public String sort(String prefix, String inputPath, int blockSize) throws IOException {

    File inputFile = new File(inputPath);
    long fileLength = inputFile.length();
    int numOfTuples = Math.round((float) fileLength / TUPLE_SIZE);
    int numOfBlocksInFile = numOfTuples / blockSize;
    int numOfBlocksInMem = (int) ((AVAILABLE_MEMORY / (blockSize * TUPLE_SIZE)) * .5f);
    int numOfTuplesInMem = numOfBlocksInMem * (int) (blockSize);
    int totalPasses = (int) Math
        .ceil(Math.log((float) fileLength / (blockSize * TUPLE_SIZE)) / Math.log(numOfBlocksInMem));
    String outputPath = "";
    for (int i = 0; i <= totalPasses; i++) {
      outputPath = path + "-" + prefix + "-" + blockSize + ".txt";
      File outputFile = new File(outputPath);
      readFile(blockSize, inputFile, outputFile);
      inputFile = new File(outputFile.toString());
      blockSize *= numOfBlocksInMem;
      // from 40 blocks read 1 tuple each
    }
    return outputPath;

  }

  private void readFile(int blockSize, File inputFile, File outputFile) throws IOException {
    try (BufferedReader br = Files.newBufferedReader(inputFile.toPath());
        BufferedWriter bw = Files.newBufferedWriter(outputFile.toPath())) {
      //br returns as stream and convert it into a List
      String line;
      ArrayList<String> pq = new ArrayList<>();
      while ((line = br.readLine()) != null) {
        pq.add(line);
        if (pq.size() == blockSize) {
          writeToFile(bw, pq);
        }
      }
      if (pq.size() > 0) {
        writeToFile(bw, pq);
      }
    }
  }

  private void writeToFile(BufferedWriter bw, ArrayList<String> pq) throws IOException {
    Comparator<String> empIdComparator = Comparator
        .comparing((String record) -> Integer.parseInt(record.substring(0, 8)))
        .thenComparing(new RecordComparator());
    pq.sort(empIdComparator);
    for (String tuple : pq) {
      bw.append(tuple).append("\n");
    }
    pq.clear();
  }

  void merge(String pathToT1, String pathToT2) throws IOException {
    Comparator<String> empIdComparator = Comparator
        .comparing((String record) -> Integer.parseInt(record.substring(0, 8)))
        .thenComparing(new RecordComparator());
    try (BufferedReader brT1 = Files.newBufferedReader(Paths.get(pathToT1));
        BufferedReader brT2 = Files.newBufferedReader(Paths.get(pathToT2));
        BufferedWriter bwT3 = Files
            .newBufferedWriter(
                Paths.get(path + "-final-no-duplicates.txt"))) {

      UniqueLineIterator file1UniqueIterator = new UniqueLineIterator(brT1);
      UniqueLineIterator file2UniqueIterator = new UniqueLineIterator(brT2);

      String T1line = file1UniqueIterator.next();
      String T2line = file2UniqueIterator.next();

      while (true) {
        if (T1line == null && T2line != null) {
          // if T1 is exhausted, dump T2
          bwT3.append(T2line).append("\n");
          T2line = file2UniqueIterator.next();
        } else if (T1line != null && T2line == null) {
          // if T2 is exhausted, dump T1
          bwT3.append(T1line).append("\n");
          T1line = file1UniqueIterator.next();
        } else if (T1line != null && T2line != null) {
          // if both arent exhausted compare them
          boolean result = empIdComparator.compare(T1line, T2line) >= 0;
          if (result) {
            bwT3.append(T1line).append("\n");
            // if tuples from both files have the same empID remove them
            if (T2line.startsWith(T1line.substring(0, 8))) {
              T2line = file2UniqueIterator.next();
            }
            T1line = file1UniqueIterator.next();
          } else {
            bwT3.append(T2line).append("\n");
            // if tuples from both files have the same empID remove them
            if (T1line.startsWith(T2line.substring(0, 8))) {
              T1line = file1UniqueIterator.next();
            }
            T2line = file2UniqueIterator.next();
          }
        } else {
          break;
        }
      }
    }

  }


}
