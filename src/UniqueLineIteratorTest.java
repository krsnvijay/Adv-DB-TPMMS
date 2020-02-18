import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UniqueLineIteratorTest {

  String path = "Employee-Generator/small-1.txt";
  private BufferedReader br;

  @AfterEach
  void tearDown() throws IOException {
    br.close();
  }

  @Test
  void testLatestRecords() throws IOException {
    br = new BufferedReader(Files.newBufferedReader(Paths.get(path)));
    ArrayList<String> actualRecords = br.lines().collect(Collectors.toCollection(ArrayList::new));
    actualRecords.remove(2);
    br = new BufferedReader(Files.newBufferedReader(Paths.get(path)));
    UniqueLineIterator uniqueLineIterator = new UniqueLineIterator(br);
    ArrayList<String> latestRecords = new ArrayList<String>();
    String line;
    while ((line = uniqueLineIterator.next()) != null) {
      latestRecords.add(line);
    }
    assertThat(String.format("Contain only the latest records from %s", path), actualRecords,
        is(equalTo(latestRecords)));
  }
}