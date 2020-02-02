public class Main {

    public static void main(String[] args) {
        System.out.println("TPMMS");
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
