package demo;

import java.io.*;

/**
 * Created by edwardlol on 2017/3/4.
 */
public class Process0901 {
    //~ Static fields/initializers ---------------------------------------------

    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    //~ Methods ----------------------------------------------------------------

    public static void main(String[] args) {
        try (FileReader fileReader = new FileReader("./datasets/bas_entry0901_end");
             BufferedReader bufferedReader = new BufferedReader(fileReader);
             FileWriter fileWriter = new FileWriter("./results/combined0901");
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {

            String line = bufferedReader.readLine();
            StringBuilder sb = new StringBuilder();
            while (line != null) {
                sb.setLength(0);
                String[] contents = line.split("@@");
                sb.append(contents[0]).append("@@").append(contents[1])
                        .append('\t').append(contents[2])
                        .append('\t').append(contents[4]);

                if (contents.length == 6) {
                    sb.append('\t').append(contents[5]);
                }
                sb.append('\n');
                bufferedWriter.write(sb.toString());
                line = bufferedReader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

// End Process0901.java
