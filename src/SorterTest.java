import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SorterTest {

  @BeforeEach
  void setUp() {
  }

  @Test
  void sort() {
  }

  @Test
  void merge() {
    String path = "Employee-Generator/small-2.txt";
  }

  @Test
  void comparatorTest() throws IOException {
    String path = "Employee-Generator/small-2.txt";
    Comparator<String> empIdComparator = Comparator
        .comparing((String record) -> Integer.parseInt(record.substring(0, 8)))
        .thenComparing(new RecordComparator());
    BufferedReader bufferedReader = new BufferedReader(Files.newBufferedReader(Paths.get(path)));
    ArrayList<String> lines = bufferedReader.lines().sorted(empIdComparator)
        .collect(Collectors.toCollection(ArrayList::new));
    ArrayList<String> expectedOrder = new ArrayList<>();
    expectedOrder.add("123456782014-03-23Ynes Puttergill          0003325918281 Clarcona FL 32710 South                    ");
    expectedOrder.add("223456792010-09-01Noland Iacobo            0009214875375 Lochgelly WV 25866 South                   ");
    expectedOrder.add("223456792012-09-01Noland  cobo            0009214875375 Lochgelly WV 25866 South                    ");
    expectedOrder.add("923456782014-03-23Ynes Puttergill          0003325918281 Clarcona FL 32710 South                    ");
    assertThat(String.format("sort based on id, date %s", path), expectedOrder,
        is(equalTo(lines)));
  }
}