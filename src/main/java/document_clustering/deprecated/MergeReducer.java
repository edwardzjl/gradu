package document_clustering.deprecated;

import document_clustering.writables.tuple_writables.IntIntTupleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class MergeReducer extends Reducer<IntIntTupleWritable, DoubleWritable, Text, DoubleWritable> {

//    private Map<String, Set<String>> map = new HashMap<>();

    private Text outputKey = new Text();


    //~ Methods ----------------------------------------------------------------

    @Override
    public void reduce(IntIntTupleWritable key, Iterable<DoubleWritable> values,
                       Context context) throws IOException, InterruptedException {

//        BiMap<String, String> srcDest = HashBiMap.create();
//        map.clear();

        // Storing each key-value pair (document) in a Map

        Iterator<DoubleWritable> iterator = values.iterator();
        DoubleWritable weight = iterator.next();

        this.outputKey.set(key.getLeftValue() + ":" + key.getRightValue());
        context.write(this.outputKey, weight);
    }
}

// End SimilarityReducer.java
