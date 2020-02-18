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

public class MultiWayMergeSortDriver {

    public static void main(String[] args) throws IOException {
        File outputFolder = new File(Constants.OUTPUT_DIR);

        Instant start = Instant.now();
        // PHASE 1 start...
        PhaseOne.phaseOne(args[0], args[1]);
        // PHASE 2 start...
        String finalFile = PhaseTwo.phaseTwo();
        // DUPLICATE ELIMINATION start...
        eliminateDuplicatesAfterMerge(outputFolder, finalFile);

        System.out.println("Total Time: " + Duration.between(start, Instant.now()).toMillis());
    }

    private static void eliminateDuplicatesAfterMerge(File outputFolder, String finalFile) throws IOException {
        Instant dupStart = Instant.now();
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(finalFile));
             BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(outputFolder + "/really_finalFINAL(1).txt"))) {
            UniqueLineIterator uniqueLineIterator = new UniqueLineIterator(bufferedReader);
            String line;
            while ((line = uniqueLineIterator.next()) != null) {
                bufferedWriter.append(line).append("\n");
            }
        }
        System.out.println("Duplicates removed in " + Duration.between(dupStart, Instant.now()).toMillis());
    }

}