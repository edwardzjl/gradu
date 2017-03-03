package demo;

import java.io.*;

/**
 * Created by edwardlol on 2017/3/3.
 */
public class Combine8703 {
    //~ Methods ----------------------------------------------------------------

    public static void main(String[] args) {
        try (FileReader fileReader = new FileReader("./datasets/end8703");
             BufferedReader bufferedReader = new BufferedReader(fileReader);
             FileWriter fileWriter = new FileWriter("./results/combined8703");
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {

            String line = bufferedReader.readLine();
            StringBuilder sb = new StringBuilder();
            while (line != null) {
                sb.setLength(0);
                String[] contents = line.split("\t");
                sb.append(contents[0]).append("@@").append(contents[1])
                        .append('\t').append(contents[2])
                        .append('\t').append(contents[4]);

                String[] nAndm = contents[5].split("@@");

                sb.append('\t').append(nAndm[0]);
                if (nAndm.length == 2) {
                    sb.append('\t').append(nAndm[1]);
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

// End Combine8703.java
