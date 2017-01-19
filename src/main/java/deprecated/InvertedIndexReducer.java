package deprecated;

import document_clustering.writables.LongSetWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * the reducer class for calculating inverted index
 * to combine all inverted indexes together
 * <p>
 * Created by edwardlol on 2016/11/25.
 */
public class InvertedIndexReducer extends Reducer<Text, LongWritable, Text, LongSetWritable> {

    protected static final AtomicInteger count = new AtomicInteger(0);

    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private LongSetWritable outputValue = new LongSetWritable();

    //~ Methods ----------------------------------------------------------------

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        Set<Long> indexes = new HashSet<>();
        for (LongWritable val : values) {
            indexes.add(val.get());
        }
        int id = count.incrementAndGet();
        this.outputKey.set(id + "::" + key.toString());
        this.outputValue.set(indexes);

        context.write(this.outputKey, this.outputValue);
    }
}

// End InvertedIndexReducer.java
