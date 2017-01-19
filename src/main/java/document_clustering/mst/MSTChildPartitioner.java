package document_clustering.mst;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by edwardlol on 2016/12/9.
 */
public class MSTChildPartitioner extends Partitioner<DoubleWritable, Text> {

    //~ Methods ----------------------------------------------------------------

    /**
     * @param key           weight
     * @param value         (doc_line_no,doc_line_no):container_id
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(DoubleWritable key, Text value, int numPartitions) {
        if (numPartitions == 0) {
            return 0;
        }
        String[] contents = value.toString().split(":");

        return Integer.valueOf(contents[1]);
    }
}

// End MSTChildPartitioner.java
