import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import Utils.LineDateComparator;
import Utils.UniqueLineIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MainTest {
    Comparator<String> empIdComparator = Comparator
            .comparing((String record) -> Integer.parseInt(record.substring(0, 8)))
            .thenComparing(new LineDateComparator());
    @Test
    void phaseOneTest() throws IOException {
        // Dont forget to limit Memory to 5m when running this test
        File outputFolder = new File(Main.outputFolder);
        Main.purge(outputFolder);
        String path = "Employee-Generator/sample-500.txt";

        BufferedReader br = new BufferedReader(Files.newBufferedReader(Paths.get(path)));
        ArrayList<String> sortedRecords = br.lines().sorted(empIdComparator).collect(Collectors.toCollection(ArrayList::new));
        br.close();
        Main.makeSublists(path);
        for (File file : Objects.requireNonNull(outputFolder.listFiles())) {
            br =  new BufferedReader(Files.newBufferedReader(file.toPath()));
            ArrayList<String> actualRecords = br.lines().collect(Collectors.toCollection(ArrayList::new));
            Assertions.assertArrayEquals(actualRecords.toArray(),sortedRecords.toArray());
        }
    }

    @Test
    void phaseTwoTest() throws IOException{
        // Dont forget to limit Memory to 2m when running this test
        File outputFolder = new File(Main.outputFolder);
        Main.purge(outputFolder);
        String path = "Employee-Generator/sample-500.txt";
        Main.makeSublists(path);
        String mergedPath = Main.phaseTwo();
        File mergedFile = new File(mergedPath);
        BufferedReader bufferedReader = Files.newBufferedReader(mergedFile.toPath());
        ArrayList<String> mergedLines = bufferedReader.lines().collect(Collectors.toCollection(ArrayList::new));
        BufferedReader bufferedReader1 = Files.newBufferedReader(Paths.get(path));
        ArrayList<String> sortedLines = bufferedReader1.lines().sorted(empIdComparator).collect(Collectors.toCollection(ArrayList::new));
        Assertions.assertArrayEquals(mergedLines.toArray(),sortedLines.toArray());

    }

    @Test
    void duplicateRemovalTest() throws IOException {
        String path = "Employee-Generator/small-1.txt";

        BufferedReader br = new BufferedReader(Files.newBufferedReader(Paths.get(path)));
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
        br.close();

    }

    @Test
    void lineComparatorTest() throws IOException {
        String path = "Employee-Generator/small-2.txt";
        Comparator<String> empIdComparator = Comparator
                .comparing((String record) -> Integer.parseInt(record.substring(0, 8)))
                .thenComparing(new LineDateComparator());
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