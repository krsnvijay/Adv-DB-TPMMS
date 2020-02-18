import Models.Employee;
import Utils.ReadUtil;
import Utils.RecordComparator;
import Utils.WriteUtil;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class Main {
    public static float preserveMemory1 = 0.15f;
    public static float preserveMemory2 = 0.0f;
    public static float preserveMemory3 = 0.12f;
    public static short maxFilesToMerge = 80;//TODO:increase to save time
    public static String outputFolder = "Employee-Generator/output/";
    public static byte tuplesPerBlock = 40;

    public static void main(String[] args) {
        File outputFolder = new File(Main.outputFolder);
        purge(outputFolder);
        Instant start = Instant.now();
        phaseOne(args[0]);
        phaseTwo();
        System.out.println("Total Time: " + Duration.between(start, Instant.now()).toMillis());
    }

    public static void purge(File outputFolder) {
        try {
            if (outputFolder.exists()) {
                for (File file : Objects.requireNonNull(outputFolder.listFiles())) {
                    if (file.isDirectory()) {
                        purge(file);
                    } else {
                        file.delete();
                    }
                }
            } else {
                outputFolder.mkdir();
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static void phaseOne(String fileOne) {
        System.out.println("Phase One Start");

        long phaseOneStart = System.nanoTime();
        int diskReads = 0;
        int diskWrites = 0;

        ReadUtil readUtil = null;
        WriteUtil writeUtil = null;

        try {
            readUtil = new ReadUtil(new File(fileOne), preserveMemory1);

            short batchCounter = 0;
            long diskReadTimer = 0;
            long diskWriteTimer = 0;

            // Repeatedly fill the M buffers with new tuples form whole file
            while (!readUtil.done) {
                System.gc();
                ArrayList<Employee> oneBatch = new ArrayList<>();

                long startTime = System.nanoTime();

                //fill blocks in one batch until run out of memory
                while (true) {
                    List<Employee> oneBlock = readUtil.readChunk();
                    //finish read or no left memory
                    if (oneBlock.isEmpty()) {
                        break;
                    }
                    oneBatch.addAll(oneBlock);
                }
                diskReadTimer += System.nanoTime() - startTime;
                // Sort the batch
                if (!oneBatch.isEmpty()) {
                    recordSort(oneBatch, 0, oneBatch.size() - 1);
                    // Dump the batch to a file
                    startTime = System.nanoTime();
                    batchCounter++;
                    writeUtil = new WriteUtil(new File(String.format(outputFolder + "%d.txt", batchCounter)));
                    writeUtil.writeChunk(oneBatch, tuplesPerBlock);
                    diskWrites += writeUtil.IOOperations;
                    writeUtil.close();
                    diskWriteTimer += System.nanoTime() - startTime;
//                    System.out.printf("Sort batch %d finish, %d tuples tn this batch %n", batchCounter, oneBatch.size());
                }
            }
            System.out.printf("Phase One Finish: Batch# = %d, IO Read Time = %.2f(s), " +
                            "IO Write Time = %.2f(s) %n", batchCounter, diskReadTimer / 1000000000.0,
                    diskWriteTimer / 1000000000.0);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (readUtil != null) {
                diskReads = readUtil.IOOperations;
                try {
                    readUtil.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (writeUtil != null) {
                try {
                    writeUtil.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.printf("Total Time: %.2f(s) %n", ((System.nanoTime() - phaseOneStart) / 1000000000.0));
        System.out.printf("Number Of I/O Read = %d, Number Of I/O Write = %d %n%n", diskReads, diskWrites);
    }

    private static boolean shouldSwap(Employee record1, Employee record2) {
        if (record1.empID < record2.empID) {
            return true;
        } else if (record1.empID == record2.empID) {
            RecordComparator rC = new RecordComparator();
            return rC.compare(record1.lastUpdated, record2.lastUpdated) < 0;
        }
        return false;
    }

    private static int partition(List<Employee> records, int low, int high) {
        Employee pivot = records.get(high);
        int i = (low - 1);
        for (int j = low; j <= high - 1; j++) {
            if (shouldSwap(records.get(j), pivot)) {
                i++;
                Employee temp = records.get(i);
                records.set(i, records.get(j));
                records.set(j, temp);
            }
        }
        Employee temp = records.get(i + 1);
        records.set((i+1), records.get(high));
        records.set(high, temp);

        return i + 1;
    }

    private static void recordSort(List<Employee> records, int low, int high) {
        if (low < high) {
            int pivot = partition(records, low, high);
            recordSort(records, low, pivot - 1);
            recordSort(records, pivot + 1, high);
        }
    }

    private static void phaseTwo() {
        System.out.println("Phase Two Start");

        long startTime2 = System.nanoTime();
        int diskReadCounter = 0;
        int diskWriteCounter = 0;

        File outputFolder = new File(Main.outputFolder);
        short passesCount = 0;

        while (outputFolder.listFiles().length > 1) {
            ++passesCount;
//            mergeSortedFiles(outputFolder, ++passesCount);
            List<ReadUtil> inputReaders = null;
            WriteUtil outputWriter = null;

            try {
                int numOfFileToMerge = Math.min(outputFolder.listFiles().length,maxFilesToMerge);
                inputReaders = new ArrayList<>(numOfFileToMerge);
                outputWriter = new WriteUtil(new File(String.format(Main.outputFolder + "merged_%s.txt", passesCount)));

                List<List<Employee>> inputBuffers = new ArrayList<>(numOfFileToMerge);
                List<Employee> outputBuffer = new ArrayList<>(tuplesPerBlock);

                short fileCount = 0;

                //create K(< M-1) input buffers and one output buffer
                for (File tempFile : outputFolder.listFiles()) {
                    if (++fileCount > numOfFileToMerge)
                        break;
                    inputReaders.add(new ReadUtil(new File(tempFile.getAbsolutePath()), preserveMemory2));
                    inputBuffers.add(new ArrayList<>());
                }

                while (true) {
                    // set all buffer empty, wait to change
                    boolean allBufferEmpty = true;

                    // 1.fills in all input buffers with one block
                    for (int i = 0; i < numOfFileToMerge; i++) {
                        List<Employee> oneBuffer = inputBuffers.get(i);
//                    System.out.println("one batch size"+oneBatch.size());
                        // finish merge this sublist, move to next
                        if (oneBuffer == null)
                            continue;
                        // one block of input buffer is empty, read next block
                        if (oneBuffer.isEmpty()) {
                            ReadUtil reader = inputReaders.get(i);
                            List<Employee> oneBlock = reader.readChunk();
                            // all records in a sublist is finish merge, set to null to ignore
                            if (oneBlock.isEmpty() && reader.done) {
                                inputBuffers.set(i, null);
                            } else {
                                allBufferEmpty = false;
                                oneBuffer.addAll(oneBlock);
//                            System.out.printf("batch %d %d %n",i,batches.get(i).size());
                            }
                        } else {
                            allBufferEmpty = false;
                        }
                    }

                    // all input buffer are empty, merge done
                    if (allBufferEmpty) {
                        // delete all temp files
                        for (int i = 0; i < numOfFileToMerge; i++) {
                            diskReadCounter += inputReaders.get(i).IOOperations;
                            inputReaders.get(i).close();
                            inputReaders.get(i).file.delete();
                        }
                        // no tuples in any input buffers, write whatever left in output buffer
                        if (!outputBuffer.isEmpty()) {
                            outputWriter.writeChunk(outputBuffer, tuplesPerBlock);
                            outputBuffer.clear();
                        }
                        break;
                    }

                    // 2.keep merging until one buffer is empty
                    while (true) {
                        boolean emptyBuffer = false;
                        Employee minClient = null;
                        short minClientIndex = -1;

                        // get local minimum among all input buffers
                        for (short i = 0; i < numOfFileToMerge; i++) {
                            List<Employee> oneBuffer = inputBuffers.get(i);
                            // this sublist done, ignore and merge rest
                            if (oneBuffer == null)
                                continue;
                            // one buffer is empty, break to above code to fill
                            if (oneBuffer.isEmpty()) {
                                emptyBuffer = true;
                                break;
                            }
                            // get the first tuple in that buffer
                            Employee firstTuple = oneBuffer.get(0);
                            if (minClient == null || minClient.empID > firstTuple.empID ) {
                                minClient = firstTuple;
                                minClientIndex = i;
                            }
                        }

                        // one buffer is empty, can not merge, go back to above code to fill
                        if (emptyBuffer)
                            break;

                        // found one local minimum among first elements of each sublist, write to file
                        outputBuffer.add(minClient);
                        inputBuffers.get(minClientIndex).remove(0);
                        if (outputBuffer.size() == tuplesPerBlock) {
                            outputWriter.writeChunk(outputBuffer, tuplesPerBlock);
                            outputBuffer.clear();
                        }
                    }//end find minimum and merge

                }//end phase two

            } catch (Exception e) {
                e.printStackTrace();
                return;
            } finally {
                if (inputReaders != null) {
                    inputReaders.forEach(reader -> {
                        try { reader.close(); }
                        catch (IOException e) { e.printStackTrace(); }
                    });
                }
                if (outputWriter != null) {
                    diskWriteCounter = outputWriter.IOOperations;
                    try { outputWriter.close(); }
                    catch (IOException e) { e.printStackTrace(); }
                }
            }
            System.out.println("min mem : "+Runtime.getRuntime().freeMemory());
        }

        System.out.printf("Phase Two Finish: Total Time = %.2f(s) %n", ((System.nanoTime() - startTime2) / 1000000000.0));
        System.out.printf("Number Of IO Read = %d, Number Of IO Write = %d %n%n", diskReadCounter, diskWriteCounter);
    }

}