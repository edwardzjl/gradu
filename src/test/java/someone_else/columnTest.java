package someone_else;

import java.awt.image.ImagingOpException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/21.
 */
public class columnTest {
    //~ Static fields/initializers ---------------------------------------------

    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    //~ Methods ----------------------------------------------------------------

    public static void main(String[] args) {
        try (FileReader fileReader = new FileReader("./datasets/init.better");
             BufferedReader bufferedReader = new BufferedReader(fileReader)){
            int docCnt = 0;
            int termCnt = 0;
            String line = bufferedReader.readLine();
            while (line != null) {
                String[] content = line.split("\t");
                String[] terms = content[1].split(" ");
                termCnt += terms.length;
                docCnt++;
                line = bufferedReader.readLine();
            }

            System.out.println("docs: " + docCnt + "; terms: " + termCnt);
        } catch (IOException e) {
                 e.printStackTrace();
        }
    }
}

// End columnTest.java
