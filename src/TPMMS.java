import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.stream.IntStream;

public class TPMMS {
  // TODO note size_of_record is also used in calculation for 'k' in phase 2
  private final double SIZE_OF_RECORD = 100.0;
  private static BufferedReader reader;
  private static BufferedWriter writer;
  private static StringBuilder opString = new StringBuilder();

  private File tempFile = new File("Employee-Generator/temp-file.txt");
  private File finalFile = new File("Employee-Generator/sorted.txt");

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
    numOfTuplesPerPage = 40;
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

    int k = (int) Math.floor((raf.length()/SIZE_OF_RECORD)/CHUNK_SIZE);
    long totalInputBuffer = (int) Math.floor(MemoryHandler.getInstance().getFreeMemory() * INPUT_PERCENT);
    int inputBuffer = (int) totalInputBuffer/k;
    int tupleCount = (int) Math.floor(inputBuffer/SIZE_OF_RECORD);

    boolean tuplesLeft = true;
    int bytePos = 0;
    int pass = 0;
    int offset = (int) Math.floor(CHUNK_SIZE*SIZE_OF_RECORD) + (pass*tupleCount);

    ArrayList<String> memContents = new ArrayList<>();
    while(bytePos < raf.length()) {
      raf.seek(bytePos);

      for(int i = 0; i < tupleCount; i++) {
        memContents.add(raf.readLine());
      }
      bytePos += offset;
    }
    System.out.println(memContents);
    //MappedByteBuffer[] mapBufArray TODO good idea with memory constraint? probably not
  }

}
