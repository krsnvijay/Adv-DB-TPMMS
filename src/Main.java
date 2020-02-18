import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class Main {
    public static float preserveMemory1 = 0.15f;
    public static float preserveMemory2 = 0.0f;
    public static float preserveMemory3 = 0.12f;
    public static short maxFilesToMerge = 80;//TODO:increase to save time
    public static String outputPath = "output/";
    public static byte tupleNumInOneBlock = 15;

    public static void main(String[] args) {
        File outputFolder = new File(outputPath);
        cleanFolder(outputFolder);

        long startTime = System.nanoTime();
        phaseOne(args[0]);
        phaseTwo();
        System.out.printf("Total Time: %.2f(s) %n", ((System.nanoTime() - startTime) / 1000000000.0));

    }

    //clean output folder
    public static void cleanFolder(File outputFolder) {
        if (outputFolder.exists()) {
            for (File file : outputFolder.listFiles()) {
                if (file.isDirectory()) {
                    cleanFolder(file);
                } else {
                    file.delete();
                }
            }
        } else {
            outputFolder.mkdir();
        }
    }

    private static void phaseOne(String fileOne) {
        System.out.println("Phase One Start");

        long startTime1 = System.nanoTime();
        int diskReadCounter = 0;
        int diskWriteCounter =0;

        FileReader inputReader = null;
        FileWriter outputWriter = null;

        try {
            inputReader = new FileReader(new File(fileOne), preserveMemory1);

            short batchCounter = 0;
            long diskReadTimer = 0;
            long diskWriteTimer = 0;

            // Repeatedly fill the M buffers with new tuples form whole file
            while (!inputReader.finish) {
                System.gc();
                ArrayList<Tuple> oneBatch = new ArrayList<>();

                long startTime = System.nanoTime();

                //fill blocks in one batch until run out of memory
                while (true) {
                    List<Tuple> oneBlock = inputReader.getOneBlock();
                    //finish read or no left memory
                    if (oneBlock.isEmpty()) {
                        break;
                    }
                    oneBatch.addAll(oneBlock);
                }
                diskReadTimer += System.nanoTime() - startTime;
                // Sort the batch
                if (!oneBatch.isEmpty()) {
                    quickSort(oneBatch, 0, oneBatch.size() - 1);
                    // Dump the batch to a file
                    startTime = System.nanoTime();
                    batchCounter++;
                    outputWriter = new FileWriter(new File(String.format(outputPath + "%d.txt", batchCounter)));
                    outputWriter.writeOneBatch(oneBatch, tupleNumInOneBlock);
                    diskWriteCounter += outputWriter.ioCounter;
                    outputWriter.close();
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
            if (inputReader != null) {
                diskReadCounter = inputReader.ioCounter;
                try {
                    inputReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (outputWriter != null) {
                try {
                    outputWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.printf("Total Time: %.2f(s) %n", ((System.nanoTime() - startTime1) / 1000000000.0));
        System.out.printf("Number Of I/O Read = %d, Number Of I/O Write = %d %n%n", diskReadCounter, diskWriteCounter);
    }

    private static void phaseTwo() {
        System.out.println("Phase Two Start");

        long startTime2 = System.nanoTime();
        int diskReadCounter = 0;
        int diskWriteCounter = 0;

        File outputFolder = new File(outputPath);
        short passesCount = 0;

        while (outputFolder.listFiles().length > 1) {
            ++passesCount;
//            mergeSortedFiles(outputFolder, ++passesCount);
            List<FileReader> inputReaders = null;
            FileWriter outputWriter = null;

            try {
                int numOfFileToMerge = Math.min(outputFolder.listFiles().length,maxFilesToMerge);
                inputReaders = new ArrayList<>(numOfFileToMerge);
                outputWriter = new FileWriter(new File(String.format(outputPath + "merged_%s.txt", passesCount)));

                List<List<Tuple>> inputBuffers = new ArrayList<>(numOfFileToMerge);
                List<Tuple> outputBuffer = new ArrayList<>(tupleNumInOneBlock);

                short fileCount = 0;

                //create K(< M-1) input buffers and one output buffer
                for (File tempFile : outputFolder.listFiles()) {
                    if (++fileCount > numOfFileToMerge)
                        break;
                    inputReaders.add(new FileReader(new File(tempFile.getAbsolutePath()), preserveMemory2));
                    inputBuffers.add(new ArrayList<>());
                }

                while (true) {
                    // set all buffer empty, wait to change
                    boolean allBufferEmpty = true;

                    // 1.fills in all input buffers with one block
                    for (int i = 0; i < numOfFileToMerge; i++) {
                        List<Tuple> oneBuffer = inputBuffers.get(i);
//                    System.out.println("one batch size"+oneBatch.size());
                        // finish merge this sublist, move to next
                        if (oneBuffer == null)
                            continue;
                        // one block of input buffer is empty, read next block
                        if (oneBuffer.isEmpty()) {
                            FileReader reader = inputReaders.get(i);
                            List<Tuple> oneBlock = reader.getOneBlock();
                            // all records in a sublist is finish merge, set to null to ignore
                            if (oneBlock.isEmpty() && reader.finish) {
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
                            diskReadCounter += inputReaders.get(i).ioCounter;
                            inputReaders.get(i).close();
                            inputReaders.get(i).file.delete();
                        }
                        // no tuples in any input buffers, write whatever left in output buffer
                        if (!outputBuffer.isEmpty()) {
                            outputWriter.writeOneBatch(outputBuffer, tupleNumInOneBlock);
                            outputBuffer.clear();
                        }
                        break;
                    }

                    // 2.keep merging until one buffer is empty
                    while (true) {
                        boolean emptyBuffer = false;
                        Tuple minClient = null;
                        short minClientIndex = -1;

                        // get local minimum among all input buffers
                        for (short i = 0; i < numOfFileToMerge; i++) {
                            List<Tuple> oneBuffer = inputBuffers.get(i);
                            // this sublist done, ignore and merge rest
                            if (oneBuffer == null)
                                continue;
                            // one buffer is empty, break to above code to fill
                            if (oneBuffer.isEmpty()) {
                                emptyBuffer = true;
                                break;
                            }
                            // get the first tuple in that buffer
                            Tuple firstTuple = oneBuffer.get(0);
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
                        if (outputBuffer.size() == tupleNumInOneBlock) {
                            outputWriter.writeOneBatch(outputBuffer, tupleNumInOneBlock);
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
                    diskWriteCounter = outputWriter.ioCounter;
                    try { outputWriter.close(); }
                    catch (IOException e) { e.printStackTrace(); }
                }
            }
            System.out.println("min mem : "+Runtime.getRuntime().freeMemory());
        }

        System.out.printf("Phase Two Finish: Total Time = %.2f(s) %n", ((System.nanoTime() - startTime2) / 1000000000.0));
        System.out.printf("Number Of IO Read = %d, Number Of IO Write = %d %n%n", diskReadCounter, diskWriteCounter);
    }

    // quick sort base on clientID
    public static void quickSort(List<Tuple> batch, int low, int high) {
        int i = low, j = high;
        Tuple pivot = batch.get(low + (high - low) / 2);
        while (i <= j) {

            while (batch.get(i).empID < pivot.empID) {
                i++;
            }
            while (batch.get(j).empID > pivot.empID) {
                j--;
            }
            if (i <= j) {
                Tuple temp = batch.get(i);
                batch.set(i, batch.get(j));
                batch.set(j, temp);
                i++;
                j--;
            }

        }
        if (low < j) {
            quickSort(batch, low, j);
        }
        if (i < high) {
            quickSort(batch, i, high);
        }
    }

}