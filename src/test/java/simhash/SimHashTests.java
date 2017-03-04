package simhash;

import document_clustering.simhash.SimHash;
import net.openhft.hashing.LongHashFunction;
import org.junit.Test;

/**
 * Created by edwardlol on 2017/3/4.
 */
public class SimHashTests {
    //~ Methods ----------------------------------------------------------------

    @Test
    public void hashDemo() {
        String sent1 = "生咖啡豆 未焙炒且未浸除咖啡碱";
        String sent2 = "生咖啡豆 未浸除咖啡碱,未焙炒";

        long tradihash1 = LongHashFunction.xx_r39().hashChars(sent1);
        long tradihash2 = LongHashFunction.xx_r39().hashChars(sent2);

        System.out.println(tradihash1);
        System.out.println(tradihash2);

        System.out.println(Long.toBinaryString(tradihash1));
        System.out.println(Long.toBinaryString(tradihash2));

        String sepedSent1 = "生 咖啡豆 未 焙炒 且 未 浸除 咖啡碱";
        String sepedSent2 = "生 咖啡豆 未 浸除 咖啡碱 未 焙炒";

        SimHash simHash1 = new SimHash(sepedSent1);
        SimHash simHash2 = new SimHash(sepedSent2);

        long simHashSig1 = simHash1.getHashCode();
        long simHashSig2 = simHash2.getHashCode();

        System.out.println(simHashSig1);
        System.out.println(simHashSig2);

        System.out.println(Long.toBinaryString(simHashSig1));
        System.out.println(Long.toBinaryString(simHashSig2));
    }

}

// End SimHashTests.java
