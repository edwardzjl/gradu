package document_clustering.simhash;

import document_clustering.util.SimHash;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class SimHashStep1Reducer extends Reducer<LongWritable, Text, IntWritable, Text> {
    //~ Static fields/initializers ---------------------------------------------

    /**
     * used to initialize ids
     */
    protected static final AtomicInteger count = new AtomicInteger(0);

    //~ Instance fields --------------------------------------------------------

    private IntWritable outputKey = new IntWritable();

    /**
     * a list of map (simhash, id)
     */
    private List<Map<String, Map<Long, Integer>>> pool = new ArrayList<>();

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

        for (Text value : values) {
            String _value = value.toString();
            String[] keyAndContent = _value.split("::");

            SimHash thisHash = new SimHash(keyAndContent[1], key.get());

            int id = contains(thisHash);
            if (id == 0) {
                id = count.incrementAndGet();
                add(thisHash, id);
            }
            this.outputKey.set(id);
            // id \t entry_id@@g_no::g_name##g_model
            context.write(this.outputKey, value);
        }
    }

    /**
     * add the input simhash to this.pool
     *
     * @param simHash the input simhash
     */
    private void add(SimHash simHash, int id) {
        String[] segments = simHash.getSegments(this.threshold + 1);

        for (int i = 0; i < this.threshold + 1; i++) {
            Map<String, Map<Long, Integer>> map = this.pool.get(i);

            Map<Long, Integer> _map = map.containsKey(segments[i]) ? map.get(segments[i]) : new HashMap<>();
            _map.put(simHash.getHashCode(), id);
            map.put(segments[i], _map);
        }
    }

    /**
     * check if this.pool contains the input simhash
     *
     * @param simHash input simhash
     * @return id if this.pool contains the input simhash, 0 otherwise
     */
    private int contains(SimHash simHash) {
        String[] segments = simHash.getSegments(this.threshold + 1);

        for (int i = 0; i < this.threshold + 1; i++) {
            Map<String, Map<Long, Integer>> map = this.pool.get(i);
            if (map.containsKey(segments[i])) {
                Map<Long, Integer> signatures = map.get(segments[i]);
                for (Map.Entry<Long, Integer> entry : signatures.entrySet()) {
                    if (simHash.hammingDistance(entry.getKey()) < this.threshold) {
                        return entry.getValue();
                    }
                }
            }
        }
        return 0;
    }
}

// End SimHashStep1Reducer.java
