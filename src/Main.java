import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {

//        long freeMem = 3;
//        int numOfTuplesPerPage = 1;
//        int ENDBYTE = 1;
//        int numOfRecords = 12;
//        int numOfInputBuffers = (int) Math.floor(freeMem/(numOfTuplesPerPage*ENDBYTE)) - 1;
//        int fileSize = numOfRecords*ENDBYTE;
//        int totalPasses = (int) Math.ceil(Math.log(fileSize/freeMem) / Math.log(numOfInputBuffers));

        if(args.length != 2){
            System.out.println("Pass Two Text files as arguments to perform TPMMS");
            System.out.println("usage: Main <path to t1> <path to t2>");
            System.exit(-1);
        }
        Instant start = Instant.now();
        TPMMS tpmms = new TPMMS();
        tpmms.runTPMMS(args[0]);
        Instant end = Instant.now();
        System.out.println("Exec time: " + Duration.between(start, end).toMillis() + "ms");
        Runtime run = Runtime.getRuntime();
        long free = run.freeMemory();
        long total = run.totalMemory();
        long max = run.maxMemory();
        long used = total - free;
        System.out.println("Memory: used " + megabyteString(used) + "M"
            + " free " + megabyteString(free) + "M"
            + " total " + megabyteString(total) + "M"
            + " max " + megabyteString(max) + "M");
    }
    private static String megabyteString(long bytes) {
        return String.format("%.1f", ((float)bytes) / 1024 / 1024);
    }
}
