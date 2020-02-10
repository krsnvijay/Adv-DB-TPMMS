import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class TPMMS {
  private static BufferedReader reader;
  private static BufferedWriter writer;

  private String filePath;

  private File tempFile = new File("Employee-Generator/temp-file.txt");

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

    char[][] lines = new char[numOfTuplesPerPage][100];
    char[] line = new char[100];

    int recordCounter = 0;
    final int startByte = 0;
    final int endByte = 100;

    while (reader.read(line, startByte, endByte) != -1) {
      System.arraycopy(line, 0, lines[recordCounter], 0, 100);
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
  }

}
