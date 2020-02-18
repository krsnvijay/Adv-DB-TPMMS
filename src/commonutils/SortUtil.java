package commonutils;

import models.Employee;

import java.util.List;

public class SortUtil {
    public static boolean shouldSwap(Employee record1, Employee record2) {
        if (record1.empID < record2.empID) {
            return true;
        } else if (record1.empID == record2.empID) {
            RecordComparator rC = new RecordComparator();
            return rC.compare(record1.lastUpdated, record2.lastUpdated) < 0;
        }
        return false;
    }

    public static int partition(List<Employee> records, int low, int high) {
        Employee pivot = records.get(high);
        int i = (low - 1);
        for (int j = low; j <= high - 1; j++) {
            if (shouldSwap(records.get(j), pivot)) {
                i++;
                Employee temp = records.get(i);
                records.set(i, records.get(j));
                records.set(j, temp);
            }
        }
        Employee temp = records.get(i + 1);
        records.set((i+1), records.get(high));
        records.set(high, temp);

        return i + 1;
    }

    public static void recordSort(List<Employee> records, int low, int high) {
        if (low < high) {
            int pivot = partition(records, low, high);
            recordSort(records, low, pivot - 1);
            recordSort(records, pivot + 1, high);
        }
    }
}
