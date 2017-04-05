package writables;

import document_clustering.writables.tuple_writables.IntIntTupleWritable;
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

    @Test
    public void intIntTupleTest() {
        IntIntTupleWritable a = new IntIntTupleWritable(1, 2);
        IntIntTupleWritable b = new IntIntTupleWritable(1, 2);

        System.out.println(a.compareTo(b));

        IntIntTupleWritable c = new IntIntTupleWritable(2, 3);
        IntIntTupleWritable d = new IntIntTupleWritable(1, 2);

        System.out.println(c.compareTo(d));

    }
}

// End WritableTests.java
