import java.util.Date;

public class Tuple {
    public int empID;
    public Date lastUpdated;
    public String empName;
    public int gender;
    public int dept;
    public String sin;
    public String address;

    public Tuple(int empID, Date lastUpdated, String empName, int gender, int dept, String sin, String address) {
        this.empID = empID;
        this.lastUpdated = lastUpdated;
        this.empName = empName;
        this.gender = gender;
        this.dept = dept;
        this.sin = sin;
        this.address = address;
    }
}
