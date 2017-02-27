package writables;

import org.junit.Test;

/**
 * Created by edwardlol on 2017/2/27.
 */
public class WritableTests {
    //~ Static fields/initializers ---------------------------------------------

    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    //~ Methods ----------------------------------------------------------------

    @Test
    public void init0901Test() {
        WritableDriver driver = new WritableDriver();
        String[] args = new String[2];
        args[0] = "/tests/in";
        args[1] = "/tests/out";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

// End WritableTests.java
