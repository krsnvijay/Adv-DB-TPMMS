import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;

public class UniqueLineIterator implements Iterator<String> {

  BufferedReader br;
  String line;
  String newLine;
  int counter = 0;
  boolean returnLine = true;

  public UniqueLineIterator(BufferedReader bufferedReader) throws IOException {
    br = bufferedReader;
    line = bufferedReader.readLine();

  }

  public String pop() {
    String temp;
    temp = line;
    line = newLine;
    return temp;
  }

  @Override
  public boolean hasNext() {
    return line != null;
  }

  @Override
  public String next() {
    try {
      newLine = br.readLine();
      if (newLine == null) {
        return pop();
      }
      while (line.startsWith(newLine.substring(0, 8))) {
        line = newLine;
        newLine = br.readLine();
        if (newLine == null) {
          break;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return pop();
  }
}
