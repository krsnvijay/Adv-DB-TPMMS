import java.io.*;
import java.util.ArrayList;

public class TPMMS {
  private static BufferedReader reader;
  private static BufferedWriter writer;

  private String filePath;

  private final int STARTBYTE = 0;
  private final int ENDBYTE = System.getProperty("os.name").toLowerCase().contains("win") ? 101 : 100;

  private File tempFile = new File("Employee-Generator/temp-file.txt");
  private File finalFile = new File("Employee-Generator/sorted.txt");

  public void runTPMMS(String filepath) {
    this.filePath = filepath;
    try {
      runPhase1();
      runPhase2();
    } catch(IOException e) {
      e.printStackTrace();
    }
  }

  private void runPhase1() throws IOException {
    sortFile(this.filePath);
  }

  private void runPhase2(){
    // nothing yet
  }

  private void performWrite(int recordCounter, char[][] lines) throws IOException {
    for(int i=0; i<recordCounter; i++) {
      writer.append(String.copyValueOf(lines[i]));
    }
  }

  private boolean shouldSwap(char[] record1, char[] record2) {
    if(Integer.parseInt(new String(record1,0,8)) <
            Integer.parseInt(new String(record2, 0, 8))) return true;

    else if(Integer.parseInt(new String(record1,0,8)) ==
            Integer.parseInt(new String(record2, 0, 8))) {
      RecordComparator rC = new RecordComparator();
      return rC.compare(String.valueOf(record1), String.valueOf(record2)) < 0;
    }
    return false;
  }

  private int partition(char[][] arr, int low, int high) {
    char[] pivot = arr[high];
    int i = (low - 1);
    for (int j = low; j <= high - 1; j++) {
    if (shouldSwap(arr[j],pivot)) {
        i++;
        char[] temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
      }
    }
    char[] temp = arr[i + 1];
    arr[i + 1] = arr[high];
    arr[high] = temp;

    return i + 1;
  }

  private void recordSort(char[][] records, int low, int high) {
    if (low < high) {
      int pivot = partition(records, low, high);
      recordSort(records, low, pivot - 1);
      recordSort(records, pivot + 1, high);
    }
  }

  private void sortFile(String filePath) throws IOException {
    reader = new BufferedReader(new FileReader(filePath));
    writer = new BufferedWriter(new FileWriter(tempFile));

    double numOfRecords = 500000.00; // pass as cmd arg

    int totalNumOfPages = (int) Math.floor(MemoryHandler.getInstance().getFreeMemory()/numOfRecords)*
            MemoryHandler.FIVE_MB;
    int numOfTuplesPerPage = (int) Math.floor(numOfRecords/(totalNumOfPages));

    char[][] lines = new char[numOfTuplesPerPage][ENDBYTE];
    char[] line = new char[ENDBYTE];

    int recordCounter = 0;

    while (reader.read(line, STARTBYTE, ENDBYTE) != -1) {
      System.arraycopy(line, 0, lines[recordCounter], 0, ENDBYTE);
      if (recordCounter == numOfTuplesPerPage-1) {
        recordSort(lines,0, recordCounter);
        performWrite(recordCounter,lines);
        recordCounter = 0;
      }
      recordCounter++;
    }
    reader.close();
    writer.close();
    System.out.printf("Blocks -- %d | Tuples/block -- %d \n",totalNumOfPages,numOfTuplesPerPage);
    
    System.out.println("Sorted lines block wise, now merging");
    mergeKWay(tempFile, 40, 0.6f);
  }

  public void mergeKWay(File tempFilePath, int CHUNK_SIZE, float INPUT_PERCENT) throws IOException {

    RandomAccessFile raf = new RandomAccessFile(tempFilePath, "r");
    writer = new BufferedWriter(new FileWriter(finalFile));

    // TODO dynamically choose appropriate buffer size
    // For 500K record file with 5MB heap max:
    // 60% of 5*1024*1024 bytes (~3.1mil bytes) with 12500 lists = input buffer divided into chunks of 251 bytes each
    // ~251 bytes per sorted list in input = 2 tuples from each list to be merged to output space (40%)
    // [this seems wrong, I've forgotten how I got this]
    // Output buffer fills after 8 passes (450000 bytes per pass), then 1 disk IO occurs
    // For entire list (5 mil bytes for 500K) we have 11 disk IOs

    int k = (int) Math.floor((raf.length()/ENDBYTE)/CHUNK_SIZE);
    long totalInputBuffer = (int) Math.floor(MemoryHandler.getInstance().getFreeMemory() * INPUT_PERCENT);
    int inputBuffer = (int) totalInputBuffer/k;
    int tupleCount = (int) Math.floor(inputBuffer/ENDBYTE);

    boolean tuplesLeft = true;
    int bytePos = 0;
    int pass = 0;
    int offset = (int) Math.floor(CHUNK_SIZE*ENDBYTE) + (pass*tupleCount);
    ArrayList<String> memContents = new ArrayList<>();
    while(bytePos < raf.length()) {
      raf.seek(bytePos);

      for(int i = 0; i < tupleCount; i++) {
        memContents.add(raf.readLine());
      }
      bytePos += offset;
    }
    //MappedByteBuffer[] mapBufArray TODO good idea with memory constraint? probably not
  }

}
