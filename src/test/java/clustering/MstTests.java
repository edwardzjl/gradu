package clustering;

import document_clustering.mst.MSTDriver;
import org.junit.Test;

/**
 * Created by edwardlol on 2017/2/27.
 */
public class MstTests {
    //~ Static fields/initializers ---------------------------------------------

    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    //~ Methods ----------------------------------------------------------------

    // my mst
    @Test
    public void mstTest() {
        MSTDriver driver = new MSTDriver();
        String[] args = new String[3];
        args[0] = "/user/edwardlol/final/price/isim";
        args[1] = "/user/edwardlol/final/price/tf_idf/docCount";
        args[2] = "/user/edwardlol/final/price/mst";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

// End MstTests.java
