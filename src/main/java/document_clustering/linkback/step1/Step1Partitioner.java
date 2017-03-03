package document_clustering.linkback.step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * partition the mapper output by the joinKey in {@link Step1KeyWritable}
 * <p>
 * Created by edwardlol on 2016/12/26.
 */
public class Step1Partitioner extends Partitioner<Step1KeyWritable, Text> {
    //~ Methods ----------------------------------------------------------------

    @Override
    public int getPartition(Step1KeyWritable step1KeyWritable, Text text, int numPartitions) {
        return step1KeyWritable.getJoinKey().hashCode() % numPartitions;
    }
}

// End Step1Partitioner.java
