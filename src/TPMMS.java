import java.io.*;
import java.util.Arrays;

public class TPMMS {
  private static BufferedReader reader;
  private static BufferedWriter writer;

  private String filePath;

  private final int numOfRecords = 500000;

  private final int STARTBYTE = 0;
  private final int ENDBYTE = System.getProperty("os.name").toLowerCase().contains("win") ? 101 : 100;

  private File tempFile = new File("Employee-Generator/temp-file.txt");
  private File finalFile = new File("Employee-Generator/sorted.txt");
  
  private int numOfTuplesPerPage;
  private int totalNumOfPages;
  private int numOfTuplesInLastBlock;

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

  private void runPhase2() throws IOException {
    mergeKWay(tempFile);
  }

  private void performWrite(int recordCounter, char[][] lines) throws IOException {
    if(recordCounter == 0) {
      writer.append(String.copyValueOf(lines[0]));
    }
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

  private void handlePageCalculations() {
    long freeMemoryEstimationPhase1 = (long) Math.ceil(MemoryHandler.getInstance().getFreeMemory());

    totalNumOfPages = (int) Math.ceil((numOfRecords * ENDBYTE) / (double) freeMemoryEstimationPhase1);
    numOfTuplesPerPage = (numOfRecords)/totalNumOfPages;

    // safeguard
    if(numOfTuplesPerPage > 12500) {
      numOfTuplesPerPage = 5000;
      totalNumOfPages = numOfRecords/numOfTuplesPerPage;
    }
  }

  private void sortFile(String filePath) throws IOException {
    reader = new BufferedReader(new FileReader(filePath));
    writer = new BufferedWriter(new FileWriter(tempFile));

    handlePageCalculations();

    char[][] lines = new char[numOfTuplesPerPage][ENDBYTE];
    char[] line = new char[ENDBYTE];
    int recordCounter = 0;

    while (reader.read(line, STARTBYTE, ENDBYTE) != -1) {
      System.arraycopy(line, 0, lines[recordCounter], 0, ENDBYTE);
      recordCounter++;
      if (recordCounter == numOfTuplesPerPage) {
        recordSort(lines,0, recordCounter-1);
        performWrite(recordCounter,lines);
        recordCounter = 0;
      }
    }

    numOfTuplesInLastBlock = recordCounter;

    if(recordCounter > 0) {
      recordSort(lines,0, recordCounter-1);
      performWrite(recordCounter,lines);
    }

    reader.close();
    writer.close();
    System.out.printf("Blocks -- %d | Tuples/block -- %d \n",totalNumOfPages,numOfTuplesPerPage);
  }

  public void mergeKWay(File tempFilePath) throws IOException {

    RandomAccessFile raf = new RandomAccessFile(tempFilePath, "r");
    writer = new BufferedWriter(new FileWriter(finalFile));

    long freeMem = MemoryHandler.getInstance().getFreeMemory();
    int numOfInputBuffers = (int) Math.floor(freeMem/(numOfTuplesPerPage*ENDBYTE)) - 1;
    int fileSize = numOfRecords*ENDBYTE;
    int totalPasses = (int) Math.ceil(Math.log(fileSize/freeMem) / Math.log(numOfInputBuffers));
    int chunkSize = numOfTuplesPerPage;
    short outIndex = 0;

    for(int pass=0; pass<totalPasses; pass++) {
      byte[][] outputBuffer = new byte[numOfTuplesPerPage][ENDBYTE];
      int[] runPointers = new int[numOfInputBuffers];
      byte[][][] buffer = new byte[numOfInputBuffers][numOfTuplesPerPage][ENDBYTE];

      for (int i = 0; i < numOfInputBuffers; i++) {
        int start = i*numOfTuplesPerPage*ENDBYTE;
        int offset = runPointers[i]*ENDBYTE;
        int seekVal = start + offset;
        raf.seek(seekVal);
        for(int j=0;j<numOfTuplesPerPage;j++) {
          raf.readFully(buffer[i][j]);
        }
      }
      while(!exhaustedBlocks(runPointers,chunkSize)) {
        int idxBlock = indexOfBlockWithMinTuple(buffer,runPointers);
        byte[] minTupleBuffer = buffer[idxBlock][runPointers[idxBlock]];
        runPointers[idxBlock] += 1;
        outputBuffer[outIndex] = minTupleBuffer;
        outIndex++;

        if(outIndex == numOfTuplesPerPage - 1) {
          for(int i=0; i<outputBuffer.length; i++) {
            writer.append(new String(outputBuffer[i]));
          }
          outIndex = 0;
        }
      }

      chunkSize *= numOfInputBuffers;

      if(outIndex > 0) {
        for(int i=0; i<outIndex; i++) {
          writer.append(new String(outputBuffer[i]));
        }
      }
    }
  }

  private int indexOfBlockWithMinTuple(byte[][][] buffer, int[] runPointers) {
    byte [][] minTuplesOfAllBlocks = new byte[buffer.length][ENDBYTE];
    int minIndex = 0;
    int lastWorkingIdx = 0 ;
    boolean setInitialLWI = false;
    for (int i = 0; i < buffer.length; i++) {
      if (runPointers[i] >= numOfTuplesPerPage) {
        continue;
      }
      if (!setInitialLWI) {
        lastWorkingIdx = i;
        setInitialLWI = true;
        continue;
      }
      minTuplesOfAllBlocks[i] = buffer[i][runPointers[i]];
      int empID1 = Integer.parseInt(new String(minTuplesOfAllBlocks[lastWorkingIdx], 0, 8));
      int empID2 = Integer.parseInt(new String(minTuplesOfAllBlocks[i], 0, 8));
      if(empID1 > empID2) {
        minIndex = i;
      }
      else if(empID1 == empID2) {
        RecordComparator rC = new RecordComparator();
        boolean res = rC.compare(new String(minTuplesOfAllBlocks[i - 1]), new String(minTuplesOfAllBlocks[i])) < 0;
        minIndex = res? i: i-1; // this is probably correct
      }
    }
    return minIndex;
  }

  private boolean exhaustedBlocks(int[] runsPerPagePointers, int maxTuples) {
    int sum = 0;
    for(int pointer:runsPerPagePointers){
      sum += pointer;
    }
    return sum == maxTuples;
  }

}
