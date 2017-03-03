package document_clustering.linkback.step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by edwardlol on 2016/12/26.
 */
public class Step2Partitioner extends Partitioner<Step2KeyWritable, Text> {
    //~ Methods ----------------------------------------------------------------

    @Override
    public int getPartition(Step2KeyWritable step2KeyWritable, Text text, int numPartitions) {
        return step2KeyWritable.getJoinKey().hashCode() % numPartitions;
    }
}

// End Step1Partitioner.java
