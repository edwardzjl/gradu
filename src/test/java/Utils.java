import org.junit.Test;

import java.text.DecimalFormat;
import java.util.Random;

/**
 * Created by edwardlol on 2017/3/1.
 */
public class Utils {
    //~ Methods ----------------------------------------------------------------

    @Test
    public void decimalFormatTest() {
        DecimalFormat decimalFormat = new DecimalFormat("0.00");
        System.out.println(decimalFormat.format(0.005));
        System.out.println(decimalFormat.toString());
    }


    @Test
    public void randomTest() {
        System.out.println(new Random(10).nextInt());
        System.out.println(new Random(10).nextInt());
        System.out.println(new Random(10).nextInt());
        System.out.println(new Random(10).nextInt());
        System.out.println(new Random(10).nextInt());
        System.out.println(new Random(9).nextInt());
        System.out.println(new Random(9).nextInt());
        System.out.println(new Random(9).nextInt());

    }


    @Test
    public void randomTest2() {
        System.out.println(new Random(10).nextInt());
        System.out.println(new Random(10).nextInt());
        System.out.println(new Random(10).nextInt());
        System.out.println(new Random(10).nextInt());
        System.out.println(new Random(10).nextInt());
        System.out.println(new Random(9).nextInt());
        System.out.println(new Random(9).nextInt());
        System.out.println(new Random(9).nextInt());

    }
}

// End Utils.java
