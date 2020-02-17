import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

public class Tuple implements Comparable<Tuple> {

  private int empId;
  private Date lastUpdated;
  private String empName;
  private boolean gender;
  private short dept;
  private int SIN;
  private String address;

  public Tuple(String line) throws ParseException {
    empId = Integer.parseInt(line.substring(0, 8));
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    lastUpdated = format.parse(line.substring(8, 18));
    empName = line.substring(18, 43).trim();
    gender = line.substring(43, 44).equals("1");
    dept = Short.parseShort(line.substring(44, 47));
    SIN = Integer.parseInt(line.substring(47, 56));
    address = line.substring(56, 99).trim();
  }

  @Override
  public String toString() {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    return String.format(
        "%8d%10s%-25s%1d%3d%9d%-43s",
        empId, format.format(lastUpdated), empName, gender ? 1 : 0, dept, SIN, address);
  }

  @Override
  public int compareTo(Tuple tuple) {
    return Comparator.comparing(Tuple::getEmpId).thenComparing(Tuple::getLastUpdated).compare(this,tuple);
  }

  public int getEmpId() {
    return empId;
  }

  public Date getLastUpdated() {
    return lastUpdated;
  }
}
