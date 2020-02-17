import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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

  void read(String path) {
    // read from a file or directory
  }

  void write(String path) {
    // write to a file or directory
  }

  void sort(String inputPath, int blockSize) throws IOException {
    File inputFile = new File(inputPath);
    long fileLength = inputFile.length();
    int numOfTuples = Math.round((float) fileLength / TUPLE_SIZE);
    int numOfBlocksInFile = numOfTuples / blockSize;
    int numOfBlocksInMem = (int) ((AVAILABLE_MEMORY / (blockSize * TUPLE_SIZE)) * .5f);
    int numOfTuplesInMem = numOfBlocksInMem * (int) (blockSize);
    int totalPasses = (int) Math
        .ceil(Math.log((float) fileLength / (blockSize * TUPLE_SIZE)) / Math.log(numOfBlocksInMem));

    for (int i = 0; i <= totalPasses; i++) {
      File outputFile = new File(path + "-" + blockSize + ".txt");
      readFile(blockSize, inputFile, outputFile);
      inputFile = new File(outputFile.toString());
      blockSize *= numOfBlocksInMem;
      // from 40 blocks read 1 tuple each
    }

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

  void merge(String path) {
    File file = new File(path);
    long fileLength = file.length();
    int numOfTuples = (int) fileLength / TUPLE_SIZE;
    int numOfBlocksInFile = numOfTuples / BLOCK_SIZE;
    int numOfBlocksInMem = (int) AVAILABLE_MEMORY / (BLOCK_SIZE * TUPLE_SIZE);
    int numOfTuplesInMem = numOfBlocksInMem * (int) (BLOCK_SIZE * .6f);
    File tempFile = new File("Employee-Generator/out/phase-2/tuples");
    // load blocks from file to input buffer//
    // merge them from low to high
  }

  void unique(String dirPath) {
    // if two ids are same keep the latest record
  }
}
