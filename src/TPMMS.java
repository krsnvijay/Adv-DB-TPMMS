import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TPMMS {

  private static BufferedReader reader;
  private static BufferedWriter writer;
  private static FileOutputStream writer2;
  private final int numOfRecords = 500000;
  private final int STARTBYTE = 0;
  private final int ENDBYTE =
      System.getProperty("os.name").toLowerCase().contains("win") ? 101 : 100;
  private String filePath;
  private int currentPass = 0;
  private int tuplesWrittenInPass = 0;

  private File tempFile = new File("Employee-Generator/sorted");
  private File finalFile = new File("Employee-Generator/sorted");

  private int numOfTuplesPerPage;
  private int totalNumOfPages;
  private int numOfTuplesInLastBlock;
  private int currentChunkPos;
  private boolean lastBlock = false;
  private Map<Integer, Integer> lastReadBuffer = new HashMap<>();
  private int maxChunks;

  public void runTPMMS(String filepath) {
    this.filePath = filepath;
    try {
      runPhase1();
      runPhase2();
    } catch (IOException e) {
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
    if (recordCounter == 0) {
      writer.append(String.copyValueOf(lines[0]));
    }
    for (int i = 0; i < recordCounter; i++) {
      writer.append(String.copyValueOf(lines[i]));
    }
  }

  private boolean shouldSwap(char[] record1, char[] record2) {
    if (Integer.parseInt(new String(record1, 0, 8)) <
        Integer.parseInt(new String(record2, 0, 8))) {
      return true;
    } else if (Integer.parseInt(new String(record1, 0, 8)) ==
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
      if (shouldSwap(arr[j], pivot)) {
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
    numOfTuplesPerPage = 40;
    totalNumOfPages = (int) Math
        .ceil(freeMemoryEstimationPhase1/(numOfTuplesPerPage * ENDBYTE));
  }

  private void sortFile(String filePath) throws IOException {
    reader = new BufferedReader(new FileReader(filePath));
    writer = new BufferedWriter(new FileWriter(tempFile + "_0.txt"));
    handlePageCalculations();

    char[][] lines = new char[numOfTuplesPerPage][ENDBYTE];
    char[] line = new char[ENDBYTE];
    int recordCounter = 0;

    while (reader.read(line, STARTBYTE, ENDBYTE) != -1) {
      System.arraycopy(line, 0, lines[recordCounter], 0, ENDBYTE);
      recordCounter++;
      if (recordCounter == numOfTuplesPerPage) {
        recordSort(lines, 0, recordCounter - 1);
        performWrite(recordCounter, lines);
        recordCounter = 0;
      }
    }

    numOfTuplesInLastBlock = recordCounter;

    if (recordCounter > 0) {
      recordSort(lines, 0, recordCounter - 1);
      performWrite(recordCounter, lines);
    }

    reader.close();
    writer.close();
    System.out.printf("Blocks -- %d | Tuples/block -- %d \n", totalNumOfPages, numOfTuplesPerPage);
  }

  public int getNumberOfJoins(int currentPass) {
    long freeMem = MemoryHandler.getInstance().getFreeMemory();
    int fileSize = numOfRecords * ENDBYTE;
    int numOfInputBuffers = (int) Math.floor(freeMem / (numOfTuplesPerPage * ENDBYTE)) - 1;
    return (int) Math.ceil(totalNumOfPages / Math.pow(numOfInputBuffers, currentPass));
  }


  public void mergeKWay(File tempFilePath) throws IOException {

    RandomAccessFile raf;

    long freeMem = MemoryHandler.getInstance().getFreeMemory();
    //int numOfInputBuffers = (int) Math.floor(freeMem / (numOfTuplesPerPage * ENDBYTE)*0.8);
    int numOfInputBuffers = 2;
    System.out.println("NUMBER OF INPUT BUFFERS IS " + numOfInputBuffers);
    int fileSize = numOfRecords * ENDBYTE;
    int totalPasses = (int) Math.ceil(Math.log(fileSize / freeMem) / Math.log(numOfInputBuffers));
    System.out.println("RUNNING FOR " + totalPasses + " PASSES");
    int chunkSize = numOfTuplesPerPage;
    int outIndex = 0;
    /*
      EXPLANATION for the loop below --
      given numOfTuplesPerPage = 5000
      numOfInputBuffers = 4
      1st Pass = 5000
      2nd Pass = 20000
      3rd Pass = 80000
      ....
     */
    for (int pass = 0; numOfRecords > chunkSize; pass++) {
      this.currentPass = pass;
      lastBlock = false;
      currentChunkPos = 0;
      maxChunks = (int) Math.floor(numOfRecords / (float) (chunkSize * numOfInputBuffers));
      raf = new RandomAccessFile(tempFile + "_" + (pass) + ".txt", "r");
      writer2 = new FileOutputStream(finalFile + "_" + (pass + 1) + ".txt");
//      writer = new BufferedWriter(new FileWriter(finalFile + "_" + (pass + 1) + ".txt"));
      byte[][] outputBuffer = new byte[numOfTuplesPerPage][ENDBYTE];
      int[] runPointers = new int[numOfInputBuffers];
      byte[][][] buffer = new byte[numOfInputBuffers][numOfTuplesPerPage][ENDBYTE];

//      // initial read alone
//      for (int i = 0; i < numOfInputBuffers; i++) {
//        int seekVal = i * chunkSize * ENDBYTE;
//        raf.seek(seekVal);
//        if ((seekVal + numOfTuplesPerPage * ENDBYTE) > fileSize) {
//          int tempSize = (fileSize - seekVal) / ENDBYTE;
//          byte[][] tempBuffer = new byte[tempSize][ENDBYTE];
//          for (int j = 0; j < tempSize; j++) {
//            raf.readFully(tempBuffer[j]);
//          }
//          buffer[i] = tempBuffer;
//          break;
//        }
//        for (int j = 0; j < numOfTuplesPerPage; j++) {
//          raf.readFully(buffer[i][j]);
//        }
//      }

      readNextBlocksInFile("", buffer, numOfTuplesPerPage, numOfInputBuffers, runPointers, chunkSize);


      // block-wise merging
      while (!exhaustedBlocks(buffer, runPointers, chunkSize, numOfInputBuffers) && !lastBlock) {

        int idxBlock = indexOfBlockWithMinTuple(buffer, runPointers, 0, -1, chunkSize);
        byte[] minTupleBuffer = Arrays
            .copyOf(buffer[idxBlock][runPointers[idxBlock] % numOfTuplesPerPage], ENDBYTE);
        runPointers[idxBlock] += 1;

        outputBuffer[outIndex] = minTupleBuffer;

        outIndex++;

        if (outIndex == numOfTuplesPerPage) {
          for (int i = 0; i < outputBuffer.length; i++) {
            writer2.write(outputBuffer[i]);
          }
          outputBuffer = new byte[numOfTuplesPerPage][ENDBYTE];
          outIndex = 0;
        }

//          if (outIndex > 0) {
//            for (int i = 0; i < outIndex; i++) {
//              writer.append(new String(outputBuffer[i]));
//            }
//          }
      }
//      }
//      catch(EOFException e) {
//        System.out.println("Hit EOF");
//        if (outIndex > 0) {
//          for (int i = 0; i < outIndex; i++) {
//            writer.append(new String(outputBuffer[i]));
//          }
//        }
//      }

      if (lastBlock) {
        int remTuples = numOfRecords % (currentChunkPos * chunkSize);
        for (int i = 0; i < remTuples; i++) {
          int idxBlock = indexOfBlockWithMinTuple(buffer, runPointers, 0, -1, chunkSize);
          byte[] minTupleBuffer = Arrays
              .copyOf(buffer[idxBlock][runPointers[idxBlock] % numOfTuplesPerPage], ENDBYTE);
          runPointers[idxBlock] += 1;
          outputBuffer[outIndex] = minTupleBuffer;
          outIndex++;

          if (outIndex == numOfTuplesPerPage) {
            for (int j = 0; j < outputBuffer.length; j++) {
              writer2.write(outputBuffer[j]);
            }
            outputBuffer = new byte[numOfTuplesPerPage][ENDBYTE];
            outIndex = 0;
          }

          int exhaustedIndex = checkBufferExhaustion(runPointers, chunkSize);
          int filledBuffers = remTuples / chunkSize;
          int partiallyFilledBuffers = remTuples % chunkSize;
          int partialBuffer = partiallyFilledBuffers > 0 ? 1 : 0;
          if (exhaustedIndex > -1 && (filledBuffers > 0 && exhaustedIndex < filledBuffers
              && runPointers[exhaustedIndex] != chunkSize
              || exhaustedIndex == (filledBuffers + partialBuffer)
              && runPointers[exhaustedIndex] != partialBuffer)) {
            readNextChunkInBuffer(buffer, exhaustedIndex, runPointers[exhaustedIndex], chunkSize);
          }
        }

        outputBuffer = new byte[numOfTuplesPerPage][ENDBYTE];
        outIndex = 0;
      }

//      if (outIndex > 0) {
//        for (int i = 0; i < outIndex; i++) {
//          writer2.write(outputBuffer[i]);
//        }
//      }
      // update disk chunk-size
      chunkSize *= numOfInputBuffers;
      raf.close();
      writer2.close();

    }

  }

  private void handleLastBlock(int[] runPointers, int chunkSize) {
    int remTuples = numOfRecords % (currentChunkPos * chunkSize);
    if (remTuples < chunkSize) {
      // if only first buffer, flush

      return;
    }
    int filledBuffers = remTuples / numOfTuplesPerPage;
    int partiallyFilledBuffers = remTuples % numOfTuplesPerPage;
    // find min
    if (partiallyFilledBuffers == 0) {

      // find min between the buffers
    }

    // if partial buffer filled, sort

  }

  private int indexOfBlockWithMinTuple(byte[][][] buffer, int[] runPointers, int currIndex,
      int minIndex, int chunkSize) {
    if (lastBlock) {
      int remTuples = numOfRecords % (currentChunkPos * chunkSize);
      int filledBuffers = remTuples / chunkSize;
      int partiallyFilledBuffers = remTuples % chunkSize;
      int partialBuffer = partiallyFilledBuffers > 0 ? 1 : 0;
      if (currIndex == filledBuffers + partialBuffer) {
        return minIndex;
      }
    }
    if (currIndex == buffer.length) {
      return minIndex;
    }
    if (runPointers[currIndex] >= chunkSize) {
      return indexOfBlockWithMinTuple(buffer, runPointers, currIndex + 1, minIndex, chunkSize);
    }
    if (minIndex == -1) {
      return indexOfBlockWithMinTuple(buffer, runPointers, currIndex + 1, currIndex, chunkSize);
    }

    int empID1 = Integer
        .parseInt(new String(buffer[minIndex][runPointers[minIndex] % numOfTuplesPerPage], 0, 8));
    int empID2 = Integer
        .parseInt(new String(buffer[currIndex][runPointers[currIndex] % numOfTuplesPerPage], 0, 8));
    if (empID1 > empID2) {
      minIndex = currIndex;
    } else if (empID1 == empID2) {
      RecordComparator rC = new RecordComparator();
      boolean res =
          rC.compare(new String(buffer[minIndex][runPointers[minIndex] % numOfTuplesPerPage]),
              new String(buffer[currIndex][runPointers[currIndex] % numOfTuplesPerPage])) < 0;
      minIndex = res ? minIndex : currIndex;
    }

    return indexOfBlockWithMinTuple(buffer, runPointers, currIndex + 1, minIndex, chunkSize);
  }

  private boolean exhaustedBlocks(byte[][][] buffer, int[] runsPerPagePointers, int maxTuples,
      int numOfInputBuffers) {

    // is the chunk (15000) exhausted?
    int sum = Arrays.stream(runsPerPagePointers).sum();

    if (sum == maxTuples * numOfInputBuffers) {
      currentChunkPos++;
      boolean result = readNextBlocksInFile(filePath, buffer, numOfTuplesPerPage, numOfInputBuffers,
          runsPerPagePointers, maxTuples);
      for (int i = 0; i < runsPerPagePointers.length; i++) {
        runsPerPagePointers[i] = 0;
      }
      lastReadBuffer.clear();
      if (currentChunkPos == maxChunks) {
        lastBlock = true;
      }
      return !result;
    }

    // is a buffer exhausted?

    int exhaustedIndex = checkBufferExhaustion(runsPerPagePointers, maxTuples);
    if (exhaustedIndex > -1 && runsPerPagePointers[exhaustedIndex] != maxTuples) {
      readNextChunkInBuffer(buffer, exhaustedIndex, runsPerPagePointers[exhaustedIndex], maxTuples);
    }

    return false;
  }

  private int checkBufferExhaustion(int[] runsPerPagePointers, int maxTuples) {

    for (int i = 0; i < runsPerPagePointers.length; i++) {
      if (runsPerPagePointers[i] == 0 || runsPerPagePointers[i] == maxTuples) {
        break;
      }
      boolean isLastBlock = false;
      if (lastBlock) {
        int remTuples = numOfRecords % (currentChunkPos * maxTuples);
        int filledBuffers = remTuples / maxTuples;
        int partiallyFilledBuffers = remTuples % maxTuples;
        int partialBuffer = partiallyFilledBuffers > 0 ? 1 : 0;
        isLastBlock =
            i == filledBuffers + partialBuffer && runsPerPagePointers[i] == partiallyFilledBuffers;
        if(isLastBlock)
          return i;
      }
      if (runsPerPagePointers[i] % numOfTuplesPerPage == 0) {
        if (lastReadBuffer.containsKey(i) && lastReadBuffer.get(i) == runsPerPagePointers[i]) {
          continue;
        }
        lastReadBuffer.put(i, runsPerPagePointers[i]);
        return i;
      }
    }
    return -1;
  }

  private boolean readNextBlocksInFile(String filePath, byte[][][] buffer, int numOfTuplesPerPage,
      int numOfInputBuffers, int[] runsPerPagePointers, int maxTuples) {
    try {
      RandomAccessFile raf = new RandomAccessFile(
          String.format("%s_%d.txt", finalFile, currentPass), "r");
      int startOfNextBlock = currentChunkPos * numOfInputBuffers * maxTuples * ENDBYTE;

      for (int i = 0; i < numOfInputBuffers; i++) {
        int start = (i * maxTuples) * ENDBYTE + startOfNextBlock; // verify this calculation
        int fileSize = numOfRecords * ENDBYTE;
        raf.seek(start);
        if ((start + (numOfTuplesPerPage * ENDBYTE)) > fileSize) {
          int tempSize = (fileSize - start) / ENDBYTE;
          if (tempSize == 0) {
            return false;
          }
          byte[][] tempBuffer = new byte[tempSize][ENDBYTE];
          for (int j = 0; j < tempSize; j++) {
            raf.readFully(tempBuffer[j]);
          }
          buffer[i] = tempBuffer;
          lastBlock = true;
          return true;
        }
        for (int j = 0; j < numOfTuplesPerPage; j++) {
          raf.readFully(buffer[i][j]);
        }
      }
      raf.close();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  // buffer[exhaustedIndex], exhaustedIndex, runsPerPagePointers[exhaustedIndex], maxTuples
  private void readNextChunkInBuffer(byte[][][] emptyBuffer, int exhaustedIndex, int pointerIndex,
      int chunkSize) {
    try {
      RandomAccessFile raf = new RandomAccessFile(
          String.format("%s_%d.txt", finalFile, currentPass), "r");
      if (lastBlock) {
        int remTuples = numOfRecords % (currentChunkPos * chunkSize);
        int filledBuffers = remTuples / chunkSize;
        int partiallyFilledBuffers = remTuples % chunkSize;
        int partialBuffer = partiallyFilledBuffers > 0 ? 1 : 0;
        boolean isLastBlock =
            exhaustedIndex == filledBuffers + partialBuffer
                && pointerIndex == partiallyFilledBuffers;
        if (isLastBlock) {
          int start =
              (((currentChunkPos * emptyBuffer.length + exhaustedIndex) * chunkSize) + pointerIndex)
                  * ENDBYTE;
          raf.seek(start);
          for (int j = 0; j < partiallyFilledBuffers; j++) {
            raf.readFully(emptyBuffer[exhaustedIndex][j]);
          }
          return;
        }
      }
      int start =
          (((currentChunkPos * emptyBuffer.length + exhaustedIndex) * chunkSize) + pointerIndex)
              * ENDBYTE;
      raf.seek(start);
      for (int j = 0; j < numOfTuplesPerPage; j++) {
        raf.readFully(emptyBuffer[exhaustedIndex][j]);
      }
      raf.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
