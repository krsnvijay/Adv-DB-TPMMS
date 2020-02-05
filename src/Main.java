import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        System.out.println("TPMMS");
        if(args.length != 2){
            System.out.println("Pass Two Text files as arguments to perform TPMMS");
            System.out.println("usage: Main <path to t1> <path to t2>");
            System.exit(-1);
        }

        TPMMS tpmms = new TPMMS();
        Runtime run = Runtime.getRuntime();
        long free = run.freeMemory();
        tpmms.sortFile(args[0],free);
        run = Runtime.getRuntime();
        free = run.freeMemory();
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
