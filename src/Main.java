import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class Main {

    public static void main(String[] args) throws IOException {
        System.out.println("TPMMS");
        if(args.length != 2){
            System.out.println("Pass Two Text files as arguments to perform TPMMS");
            System.out.println("usage: Main <path to t1> <path to t2>");
            System.exit(-1);
        }

        Instant s = Instant.now();
        TPMMS tpmms = new TPMMS();
        tpmms.sortFile(args[0]);
        Instant e = Instant.now();
        System.out.println(Duration.between(s, e).toMillis());
    }
    private static String megabyteString(long bytes) {
        return String.format("%.1f", ((float)bytes) / 1024 / 1024);
    }
}
