package document_clustering.simhash;


import net.openhft.hashing.LongHashFunction;

import java.util.StringTokenizer;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class SimHash {
    //~ Instance fields --------------------------------------------------------

    private final String tokens;

    private final long hashCode;

    private final int hashbits;

    //~ Constructors -----------------------------------------------------------

    /**
     * init a simhash instance of the token using 64 bit length as default
     * @param tokens
     */
    public SimHash(String tokens) {
        this(tokens, 64);
    }

    /**
     * main constructor
     * @param tokens
     * @param hashbits
     */
    public SimHash(String tokens, int hashbits) {
        this.tokens = tokens;
        this.hashbits = hashbits;

        int[] features = new int[this.hashbits];

        StringTokenizer stringTokens = new StringTokenizer(this.tokens);
        while (stringTokens.hasMoreTokens()) {
            String term = stringTokens.nextToken();
            // TODO: 2016/12/10 add 32 bit hash support
            long termHash = LongHashFunction.xx_r39().hashChars(term);

            for (int i = 0; i < this.hashbits; i++) {
                long bitmask = 1L << i;
                features[i] += (termHash & bitmask) != 0 ? 1 : -1;
            }
        }

        long fingerprint = 0L;
        for (int i = 0; i < this.hashbits; i++) {
            if (features[i] >= 0) {
                fingerprint = fingerprint | (1L << i);
            }
        }
        this.hashCode = fingerprint;
    }

    /**
     *
     * @param tokens
     * @param hashCode
     */
    public SimHash(String tokens, long hashCode) {
        this(tokens, hashCode, 64);
    }

    /**
     *
     * @param tokens
     * @param hashCode
     * @param hashbits
     */
    public SimHash(String tokens, long hashCode, int hashbits) {
        this.tokens = tokens;
        this.hashCode = hashCode;
        this.hashbits = hashbits;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * calculate the hamming distance of the two input hashcode
     * @param hash1 hash1
     * @param hash2 hash2
     * @return the hamming distance of the two input hashcode
     */
    public static int hammingDistance(long hash1, long hash2) {
        return Long.bitCount(hash1 ^ hash2);
    }

    private static String[] getSegments(long signature, int segmentNumber) {
        String[] result = new String[segmentNumber];

        // TODO: 2016/12/10 hard code 64 is not good
        String strSig = String.format("%64s", Long.toBinaryString(signature)).replace(' ', '0');
        int segmentLength = 64 / segmentNumber;
        for (int i = 0; i < segmentNumber; i++) {
            String tmp = strSig.substring(i * segmentLength, (i + 1) * segmentLength - 1);
            result[i] = tmp;
        }
        return result;
    }

    public int hammingDistance(SimHash other) {
        return hammingDistance(other.getHashCode());
    }

    public int hammingDistance(long thatHash) {
        return hammingDistance(this.hashCode, thatHash);
    }

    public String getTokens() {
        return this.tokens;
    }

    public long getHashCode() {
        return this.hashCode;
    }

    public int getHashbits() {
        return this.hashbits;
    }

    public String[] getSegments(int segmentNumber) {
        return getSegments(this.hashCode, segmentNumber);
    }

}

// End SimHash.java
