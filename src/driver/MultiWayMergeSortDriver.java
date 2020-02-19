package driver;

import commonutils.Constants;
import commonutils.UniqueLineIterator;
import phaseone.PhaseOne;
import phasetwo.PhaseTwo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public class MultiWayMergeSortDriver {

    public static void main(String[] args) throws IOException {
        System.out.println("T1 file @ "+args[0]);
        System.out.println("T2 file @ "+args[1]);
        File outputFolder = new File(Constants.OUTPUT_DIR);
        purge(outputFolder);
        Instant start = Instant.now();
        // PHASE 1 start...
        PhaseOne.phaseOne(args[0], args[1]);
        // PHASE 2 start...
        String finalFile = PhaseTwo.phaseTwo();
        // DUPLICATE ELIMINATION start...
        eliminateDuplicatesAfterMerge(outputFolder, finalFile);
        // To purge the final merged file
        purgeFinalMergedFile(finalFile);
        System.out.println("-- Final Metrics --");
        System.out.println("Total Exec. Time -- " + Duration.between(start, Instant.now()).toMillis() / Constants.TIME_CALC_FACTOR + " seconds");
        System.out.println("Final Output File @ " + outputFolder + "\\FINAL_OUTPUT.txt");
        System.out.println("Free Memory @ end of execution -- " + Runtime.getRuntime().freeMemory()/(1024*1024) + "MB");

    }

    private static void purgeFinalMergedFile(String finalFile) {
        File mergedFileToDelete = new File(finalFile);
        mergedFileToDelete.delete();
    }

    private static void eliminateDuplicatesAfterMerge(File outputFolder, String finalFile) throws IOException {
        Instant dupStart = Instant.now();
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(finalFile));
             BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(outputFolder + "/FINAL_OUTPUT.txt"))) {
            UniqueLineIterator uniqueLineIterator = new UniqueLineIterator(bufferedReader);
            String line;
            while ((line = uniqueLineIterator.next()) != null) {
                bufferedWriter.append(line).append("\n");
            }
            System.out.println("Total Duplicates removed -- " + uniqueLineIterator.getCounter());

        }
        System.out.println("Duplicates removed in -- " + Duration.between(dupStart, Instant.now()).toMillis() / Constants.TIME_CALC_FACTOR + " seconds");
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
        } catch (Exception e) {
            System.out.println("Error processing the file:" + e.getMessage());

        }
    }

}