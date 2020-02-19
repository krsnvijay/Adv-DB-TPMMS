package phaseone;

import commonutils.Constants;
import models.Employee;
import commonutils.ReadUtil;
import commonutils.WriteUtil;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static commonutils.SortUtil.recordSort;

public class PhaseOne {
    private static short batchCounter = 0;

    public static void phaseOne(String fileOne, String fileTwo) {
        System.out.println("-- PHASE ONE --");
        System.out.println("RUNNING .....");
        Instant phaseOneStart = Instant.now();
        int ioReads = 0;
        int ioWrites = 0;
        long diskReadTimer = 0;
        long diskWriteTimer = 0;

        String phaseOneMetricsFile1 = makeSublists(fileOne, ioReads, ioWrites, diskReadTimer, diskWriteTimer);
        String phaseOneMetricsFile2 = makeSublists(fileTwo, ioReads, ioWrites, diskReadTimer, diskWriteTimer);
        diskReadTimer = Long.parseLong(phaseOneMetricsFile1.split(" ")[0]) + Long.parseLong(phaseOneMetricsFile2.split(" ")[0]);
        diskWriteTimer = Long.parseLong(phaseOneMetricsFile1.split(" ")[1]) + Long.parseLong(phaseOneMetricsFile2.split(" ")[1]);
        ioReads = Integer.parseInt(phaseOneMetricsFile1.split(" ")[2]) + Integer.parseInt(phaseOneMetricsFile2.split(" ")[2]);
        ioWrites = Integer.parseInt(phaseOneMetricsFile1.split(" ")[3]) + Integer.parseInt(phaseOneMetricsFile2.split(" ")[3]);
        System.out.printf("-- Phase One Metrics --\nNumber Of Temp. Sorted Files written - %d\n" +
                        "Exec. Time -- %.4f seconds\n" +
                        "IO Reads took %.4f seconds\n" +
                        "IO Writes took %.4f seconds\n" +
                        "Total Disk Reads -- %d\n" +
                        "Total Disk Writes -- %d\n" +
                        "~ PHASE ONE END ~\n",
                batchCounter,
                Duration.between(phaseOneStart, Instant.now()).toMillis() / Constants.TIME_CALC_FACTOR,
                diskReadTimer / Constants.TIME_CALC_FACTOR,
                diskWriteTimer / Constants.TIME_CALC_FACTOR,
                ioReads,
                ioWrites);
    }

    public static String makeSublists(String file, int ioReads, int ioWrites, long ioReadTimer, long ioWriteTimer) {
        try (ReadUtil readUtil = new ReadUtil(new File(file))){
            while (!readUtil.done) {
                System.gc();
                ArrayList<Employee> oneBatch = new ArrayList<>();

                Instant startTime = Instant.now();
                while (true) {
                    List<Employee> oneBlock = readUtil.readChunk();
                    if (oneBlock.isEmpty()) {
                        break;
                    }
                    oneBatch.addAll(oneBlock);
                }
                ioReadTimer += Duration.between(startTime, Instant.now()).toMillis();
                if (!oneBatch.isEmpty()) {
                    recordSort(oneBatch, 0, oneBatch.size() - 1);
                    startTime = Instant.now();
                    batchCounter++;
                    try(WriteUtil writeUtil = new WriteUtil(new File(String.format(Constants.OUTPUT_DIR + "sublist-%d.txt", batchCounter)))) {
                        writeUtil.writeChunk(oneBatch, Constants.TUPLES_PER_BLOCK);
                        ioWrites += writeUtil.IOOperations;
                        writeUtil.close();
                        ioWriteTimer += Duration.between(startTime, Instant.now()).toMillis();
                    }
                }
            }
            ioReads = readUtil.IOOperations;
        } catch (IOException e) {
            System.out.println("Error processing the file:" + e.getMessage());
        }
        return String.format("%d %d %d %d", ioReadTimer, ioWriteTimer, ioReads, ioWrites);
    }

}
