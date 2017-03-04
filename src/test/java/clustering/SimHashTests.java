package clustering;

import benchmark.tools.NonSimHashDriver;
import document_clustering.simhash.SimHashDriver;
import org.junit.Test;

/**
 * Created by edwardlol on 17-2-22.
 */
public class SimHashTests {

    @Test
    public void simHash() {
        SimHashDriver driver = new SimHashDriver();
        String[] args = new String[2];
        args[0] = "/init/out";
        args[1] = "/simhash";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void nonSimHash() {
        NonSimHashDriver driver = new NonSimHashDriver();
        String[] args = new String[2];
        args[0] = "/final/init/out/0901";
        args[1] = "/final/nonSimhash/0901";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
