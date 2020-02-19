package phasetwo;

import commonutils.Constants;
import commonutils.ReadUtil;
import commonutils.SortUtil;
import commonutils.WriteUtil;
import models.Employee;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class PhaseTwo {
    static boolean areInputBuffersEmpty = true;

    public static String phaseTwo() {
        System.out.println("-- PHASE TWO --");
        System.out.println("RUNNING .....");
        Instant startTime2 = Instant.now();
        int ioReads = 0;
        int ioWrites = 0;

        File outputFolder = new File(Constants.OUTPUT_DIR);
        int passes = 0;


        while (outputFolder.listFiles().length > 1) {
            ArrayList<ReadUtil> readUtils = null;
            passes++;

            try (WriteUtil writeUtil = new WriteUtil(new File(String.format(Constants.OUTPUT_DIR + "sorted_sublist-%s.txt", passes)))) {
                int mergableSublists = Math.min(outputFolder.listFiles().length, Constants.MAX_FILES_TO_MERGE);
                readUtils = new ArrayList<>(mergableSublists);

                ArrayList<ArrayList<Employee>> inputBuffers = new ArrayList<>(mergableSublists);
                ArrayList<Employee> outputBuffer = new ArrayList<Employee>(Constants.TUPLES_PER_BLOCK);

                int sublistCounter = 0;
                for (File tempFile : outputFolder.listFiles()) {
                    if (++sublistCounter > mergableSublists)
                        break;
                    readUtils.add(new ReadUtil(new File(tempFile.getAbsolutePath())));
                    inputBuffers.add(new ArrayList<Employee>());
                }
                while (true) {
                    areInputBuffersEmpty = true;
                    readBuffersWithFirstBlock(readUtils, mergableSublists, inputBuffers);
                    if (areInputBuffersEmpty) {
                        ioReads = handleEmptyBuffers(ioReads, readUtils, writeUtil, mergableSublists, outputBuffer);
                        break;
                    }
                    while (true) {
                        if (mergeUntilBufferExhaustion(writeUtil, mergableSublists, inputBuffers, outputBuffer)) break;

                    }
                }
                ioWrites = writeUtil.IOOperations;
                for (ReadUtil reader : readUtils) {
                        reader.close();
                }

            } catch (Exception e) {
                System.out.println("Error processing the file:" + e.getMessage());
                return null;
            }
        }
        System.out.printf("-- Phase Two Metrics --\n" +
                        "Exec. Time -- %.4f seconds\n" +
                        "Total Disk Reads -- %d\n" +
                        "Total Disk Writes -- %d\n" +
                        "~ PHASE TWO END ~\n",
                Duration.between(startTime2, Instant.now()).toMillis() / Constants.TIME_CALC_FACTOR,
                ioReads,
                ioWrites);
        return String.format(Constants.OUTPUT_DIR + "sorted_sublist-%s.txt", passes);
    }

    private static boolean mergeUntilBufferExhaustion(WriteUtil writeUtil, int mergableSublists, ArrayList<ArrayList<Employee>> inputBuffers, ArrayList<Employee> outputBuffer) {
        boolean exhausted = false;
        Employee lowestRecord = null;
        int lowestRecordIndex = -1;
        for (int i = 0; i < mergableSublists; i++) {
            ArrayList<Employee> currentInputBuffer = inputBuffers.get(i);
            if (currentInputBuffer == null)
                continue;
            if (currentInputBuffer.isEmpty()) {
                exhausted = true;
                break;
            }
            Employee firstRecord = currentInputBuffer.get(0);
            if (lowestRecord == null || SortUtil.shouldSwap(firstRecord, lowestRecord)) {
                lowestRecord = firstRecord;
                lowestRecordIndex = i;
            }
        }
        if (exhausted)
            return true;
        outputBuffer.add(lowestRecord);
        inputBuffers.get(lowestRecordIndex).remove(0);
        if (outputBuffer.size() == Constants.TUPLES_PER_BLOCK) {
            writeUtil.writeChunk(outputBuffer, Constants.TUPLES_PER_BLOCK);
            outputBuffer.clear();
        }
        return false;
    }

    private static int handleEmptyBuffers(int ioReads, ArrayList<ReadUtil> readUtils, WriteUtil writeUtil, int mergableSublists, ArrayList<Employee> outputBuffer) throws IOException {
        for (int i = 0; i < mergableSublists; i++) {
            ioReads += readUtils.get(i).IOOperations;
            readUtils.get(i).close();
            readUtils.get(i).file.delete();
        }
        if (!outputBuffer.isEmpty()) {
            writeUtil.writeChunk(outputBuffer, Constants.TUPLES_PER_BLOCK);
            outputBuffer.clear();
        }

        return ioReads;
    }

    private static void readBuffersWithFirstBlock(List<ReadUtil> inputReaders, int numOfFileToMerge, ArrayList<ArrayList<Employee>> inputBuffers) {
        for (int i = 0; i < numOfFileToMerge; i++) {
            List<Employee> currentInputBuffer = inputBuffers.get(i);
            if (currentInputBuffer == null)
                continue;
            if (currentInputBuffer.isEmpty()) {
                ReadUtil reader = inputReaders.get(i);
                List<Employee> oneBlock = reader.readChunk();
                if (oneBlock.isEmpty() && reader.done) {
                    inputBuffers.set(i, null);
                } else {
                    areInputBuffersEmpty = false;
                    currentInputBuffer.addAll(oneBlock);
                }
            } else {
                areInputBuffersEmpty = false;
            }
        }
    }
}