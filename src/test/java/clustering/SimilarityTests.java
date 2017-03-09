package clustering;

import document_clustering.similarity.ISimDriver;
import document_clustering.similarity.PreDriver;
import org.junit.Test;

/**
 * Created by edwardlol on 17-2-23.
 */
public class SimilarityTests {

    @Test
    public void preTest() {
        PreDriver driver = new PreDriver();
        String[] args = new String[3];
        args[0] = "/0901/iindex/result";
        args[1] = "/0901/pre";
        args[2] = "0";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void isimTest() {
        ISimDriver driver = new ISimDriver();
        String[] args = new String[4];
        args[0] = "/pre";
        args[1] = "/isim";
        args[2] = "0";
        args[3] = "7";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void containerTest() {

    }
}
