package document_clustering.similarity;

import document_clustering.writables.tuple_writables.IntIntTupleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by edwardlol on 2017/2/27.
 */
public class PrePartitioner extends Partitioner<IntIntTupleWritable, Text> {
    //~ Static fields/initializers ---------------------------------------------

    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    //~ Methods ----------------------------------------------------------------


    @Override
    public int getPartition(IntIntTupleWritable key, Text value, int numPartitions) {
        return (key.getLeftValue() & Integer.MAX_VALUE) % numPartitions;

    }
}

// End PrePartitioner.java
