package commonutils;

import java.util.Comparator;
import java.util.Date;

public class RecordComparator implements Comparator<Date> {

  @Override
  public int compare(Date lastUpdatedA, Date lastUpdatedB) {
    return lastUpdatedA.compareTo(lastUpdatedB);
  }
}
