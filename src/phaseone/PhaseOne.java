package phaseone;

import commonutils.Constants;
import models.Employee;
import commonutils.ReadUtil;
import commonutils.WriteUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static commonutils.SortUtil.recordSort;

public class PhaseOne {
    private static short batchCounter = 0;

    public static void phaseOne(String fileOne, String fileTwo) {
        System.out.println("Phase One Start");
        long phaseOneStart = System.nanoTime();
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
                        "Exec. Time -- %.4f\n" +
                        "IO Reads took %.4f seconds\n" +
                        "IO Writes took %.4f seconds\n" +
                        "Total Disk Reads -- %d\n" +
                        "Total Disk Writes -- %d\n",
                batchCounter,
                (System.nanoTime() - phaseOneStart) / Constants.TIME_CALC_FACTOR,
                diskReadTimer / Constants.TIME_CALC_FACTOR,
                diskWriteTimer / Constants.TIME_CALC_FACTOR,
                ioReads,
                ioWrites);
    }

    private static String makeSublists(String file, int ioReads, int ioWrites, long ioReadTimer, long ioWriteTimer) {
        try (ReadUtil readUtil = new ReadUtil(new File(file))){
            while (!readUtil.done) {
                System.gc();
                ArrayList<Employee> oneBatch = new ArrayList<>();

                long startTime = System.nanoTime();
                while (true) {
                    List<Employee> oneBlock = readUtil.readChunk();
                    if (oneBlock.isEmpty()) {
                        break;
                    }
                    oneBatch.addAll(oneBlock);
                }
                ioReadTimer += System.nanoTime() - startTime;
                if (!oneBatch.isEmpty()) {
                    recordSort(oneBatch, 0, oneBatch.size() - 1);
                    startTime = System.nanoTime();
                    batchCounter++;
                    try(WriteUtil writeUtil = new WriteUtil(new File(String.format(Constants.OUTPUT_DIR + "%d.txt", batchCounter)))) {
                        writeUtil.writeChunk(oneBatch, Constants.TUPLES_PER_BLOCK);
                        ioWrites += writeUtil.IOOperations;
                        writeUtil.close();
                        ioWriteTimer += System.nanoTime() - startTime;
                    }
                }
            }
            ioReads = readUtil.IOOperations;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return String.format("%d %d %d %d", ioReadTimer, ioWriteTimer, ioReads, ioWrites);
    }

}
