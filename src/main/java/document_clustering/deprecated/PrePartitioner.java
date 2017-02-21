package document_clustering.deprecated;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by edwardlol on 2016/12/27.
 */
public class PrePartitioner extends Partitioner<Text, Text> {
    //~ Static fields/initializers ---------------------------------------------

    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    //~ Methods ----------------------------------------------------------------

    @Override
    public int getPartition(Text text, Text text2, int numPartitions) {
        return 0;
    }
}

// End PrePartitioner.java
