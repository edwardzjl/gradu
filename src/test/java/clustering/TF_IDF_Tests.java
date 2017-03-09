package clustering;

import document_clustering.tf_idf.TF_IDF_Driver;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by edwardlol on 17-2-22.
 */
public class TF_IDF_Tests {

    /**
     * calculate each term's tf-idf of every document
     */
    @Test
    public void tf_idf() {
        TF_IDF_Driver driver = new TF_IDF_Driver();
        String[] args = new String[2];
        args[0] = "/0901/simhash";
        args[1] = "/0901/tf_idf";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void demo() {
        Map<Integer, String> map = new HashMap<>();
        try (FileReader fileReader = new FileReader("datasets/invertedIndex");
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {

            String line = bufferedReader.readLine();
            while (line != null) {
                String[] content = line.split("\t");
                String[] idAndTerm = content[0].split("::");
                map.put(Integer.valueOf(idAndTerm[0]), idAndTerm[1]);
                line = bufferedReader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (FileReader fileReader = new FileReader("datasets/vectors");
             BufferedReader bufferedReader = new BufferedReader(fileReader);
             FileWriter fileWriter = new FileWriter("datasets/result");
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {

            String line = bufferedReader.readLine();
            while (line != null) {
                String[] content = line.split("\t");
                int docId = Integer.valueOf(content[0]);
                String[] _strVector = content[1].split("\\{");
                String strVector = _strVector[1].replace("}", "");
                String[] elements = strVector.split(",");
                bufferedWriter.append(String.valueOf(docId)).append(": ");
                for (String element : elements) {
                    String[] indexAndValue = element.split(":");
                    String term = map.get(Integer.valueOf(indexAndValue[0]));
                    bufferedWriter.append(term).append(":")
                            .append(String.format("%.2f", Double.valueOf(indexAndValue[1])))
                            .append(", ");
                }
                bufferedWriter.append("\n");
                bufferedWriter.flush();
                line = bufferedReader.readLine();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
