package deprecated;

import document_clustering.util.CollectionUtil;
import document_clustering.util.SimHash;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class SimHashReducer extends Reducer<LongWritable, Text, IntWritable, Text> {
    //~ Static fields/initializers ---------------------------------------------

    /**
     * used to initialize ids
     */
    protected static final AtomicInteger count = new AtomicInteger(0);

    //~ Instance fields --------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    private Text outputValue = new Text();

    private List<Map<String, Set<Long>>> pool = new ArrayList<>();

    private int threshold;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.threshold = context.getConfiguration().getInt("simhash.threshold", 3);

        for (int i = 0; i < this.threshold + 1; i++) {
            this.pool.add(new HashMap<>());
        }
    }

    /**
     * @param key     simhash
     * @param values  entry_id@@g_no::g_name##g_model
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String value = values.iterator().next().toString();
        String[] keyAndContent = value.split("::");

        SimHash thisHash = new SimHash(keyAndContent[1], key.get());

        if (!contains(thisHash)) {
            add(thisHash);

            int id = count.incrementAndGet();
            this.outputKey.set(id);
            this.outputValue.set(value);
            context.write(this.outputKey, this.outputValue);
        }
    }

    /**
     * add the input simhash to this.pool
     *
     * @param simHash the input simhash
     */
    private void add(SimHash simHash) {
        String[] segments = simHash.getSegments(this.threshold + 1);

        for (int i = 0; i < this.threshold + 1; i++) {
            Map<String, Set<Long>> map = this.pool.get(i);

            CollectionUtil.updateSetMap(map, segments[i], simHash.getHashCode());
        }
    }

    /**
     * check if this.pool contains the input simhash
     *
     * @param simHash input simhash
     * @return true if this.pool contains the input simhash
     */
    private boolean contains(SimHash simHash) {
        String[] segments = simHash.getSegments(this.threshold + 1);

        for (int i = 0; i < this.threshold + 1; i++) {
            Map<String, Set<Long>> map = this.pool.get(i);
            if (map.containsKey(segments[i])) {
                Set<Long> signatures = map.get(segments[i]);
                for (long signature : signatures) {
                    if (simHash.hammingDistance(signature) < this.threshold) {
                        return true;
                    }
                }
            }
        }
//        for (String segment : segments) {
//            for (Map<String, Set<Long>> map : this.pool) {
//                if (map.containsKey(segment)) {
//                    Set<Long> signatures = map.get(segment);
//                    for (long signature : signatures) {
//                        if (simHash.hammingDistance(signature) < this.threshold) {
//                            return true;
//                        }
//                    }
//                }
//            }
//        }
        return false;
    }
}

// End SimHashStep1Reducer.java
