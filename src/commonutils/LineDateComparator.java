package commonutils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

public class LineDateComparator implements Comparator<String> {


    @Override
    public int compare(String recordA, String recordB) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date recADate = simpleDateFormat.parse(recordA.substring(8, 18));
            Date recBDate = simpleDateFormat.parse(recordB.substring(8, 18));
            return recADate.compareTo(recBDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

}
