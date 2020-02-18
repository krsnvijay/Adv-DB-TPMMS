package phasetwo;

import commonutils.Constants;
import commonutils.ReadUtil;
import commonutils.SortUtil;
import commonutils.WriteUtil;
import models.Employee;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PhaseTwo {
    static boolean allBufferEmpty = true;
    public static String phaseTwo() {
        System.out.println("Phase Two Start");
        long startTime2 = System.nanoTime();
        int diskReadCounter = 0;
        int diskWriteCounter = 0;

        File outputFolder = new File(Constants.OUTPUT_DIR);
        short passesCount = 0;


        while (outputFolder.listFiles().length > 1) {
            ++passesCount;
//            mergeSortedFiles(outputFolder, ++passesCount);
            List<ReadUtil> inputReaders = null;

            try(WriteUtil outputWriter = new WriteUtil(new File(String.format(Constants.OUTPUT_DIR + "merged_%s.txt", passesCount))) ) {
                int numOfFileToMerge = Math.min(outputFolder.listFiles().length, Constants.MAX_FILES_TO_MERGE);
                inputReaders = new ArrayList<ReadUtil>(numOfFileToMerge);

                List<List<Employee>> inputBuffers = new ArrayList<List<Employee>>(numOfFileToMerge);
                List<Employee> outputBuffer = new ArrayList<Employee>(Constants.TUPLES_PER_BLOCK);

                short fileCount = 0;

                //create K(< M-1) input buffers and one output buffer
                for (File tempFile : outputFolder.listFiles()) {
                    if (++fileCount > numOfFileToMerge)
                        break;
                    inputReaders.add(new ReadUtil(new File(tempFile.getAbsolutePath())));
                    inputBuffers.add(new ArrayList<Employee>());
                }

                while (true) {
                    // set all buffer empty, wait to change
                    allBufferEmpty = true;
                    // 1.fills in all input buffers with one block
                    readBuffersWithFirstBlock(inputReaders, numOfFileToMerge, inputBuffers);

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
                            outputWriter.writeChunk(outputBuffer, Constants.TUPLES_PER_BLOCK);
                            outputBuffer.clear();
                        }
                        break;
                    }

                    // 2.keep merging until one buffer is empty
                    while (true) {

                        boolean emptyBuffer = false;
                        Employee minTuple = null;
                        short minTupleIndex = -1;

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
                            if (minTuple == null || SortUtil.shouldSwap(firstTuple, minTuple)) {
                                minTuple = firstTuple;
                                minTupleIndex = i;
                            }
                        }

                        // one buffer is empty, can not merge, go back to above code to fill
                        if (emptyBuffer)
                            break;

                        // found one local minimum among first elements of each sublist, write to file
                        outputBuffer.add(minTuple);
                        inputBuffers.get(minTupleIndex).remove(0);
                        if (outputBuffer.size() == Constants.TUPLES_PER_BLOCK) {
                            outputWriter.writeChunk(outputBuffer, Constants.TUPLES_PER_BLOCK);
                            outputBuffer.clear();
                        }

                    }//end find minimum and merge

                }//end phase two
                diskWriteCounter = outputWriter.IOOperations;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            } finally {
                if (inputReaders != null) {
                    inputReaders.forEach(reader -> {
                        try {
                            reader.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                }
            }
            System.out.println("min mem : " + Runtime.getRuntime().freeMemory());
        }

        System.out.printf("Phase Two Finish: Total Time = %.2f(s) %n", ((System.nanoTime() - startTime2) / 1000000000.0));
        System.out.printf("Number Of IO Read = %d, Number Of IO Write = %d %n%n", diskReadCounter, diskWriteCounter);
        return String.format(Constants.OUTPUT_DIR + "merged_%s.txt", passesCount);
    }

    private static void readBuffersWithFirstBlock(List<ReadUtil> inputReaders, int numOfFileToMerge, List<List<Employee>> inputBuffers) {
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
    }
}