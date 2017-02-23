package clustering;

import document_clustering.similarity.PreDriver2;
import org.junit.Test;

/**
 * Created by edwardlol on 17-2-23.
 */
public class SimilarityTests {

    @Test
    public void preTest() {
        PreDriver2 driver = new PreDriver2();
        String[] args = new String[3];
        args[0] = "/final/iindex/0901_id/result";
        args[1] = "/final/pre/0901_v2_2";
        args[2] = "0";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
