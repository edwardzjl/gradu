import org.junit.Test;

import java.text.DecimalFormat;

/**
 * Created by edwardlol on 2017/3/1.
 */
public class Utils {
    //~ Static fields/initializers ---------------------------------------------

    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    //~ Methods ----------------------------------------------------------------

    @Test
    public void decimalFormatTest() {
        DecimalFormat decimalFormat = new DecimalFormat("0.00");
        System.out.println(decimalFormat.format(0.005));
        System.out.println(decimalFormat.toString());
    }
}

// End Utils.java
