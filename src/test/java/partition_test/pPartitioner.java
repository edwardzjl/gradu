package partition_test;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by edwardlol on 2016/12/9.
 */
public class pPartitioner extends Partitioner<Text, Text> {
    //~ Static fields/initializers ---------------------------------------------

    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    //~ Methods ----------------------------------------------------------------


    @Override
    public int getPartition(Text key, Text value, int numPartitions) {

        String[] str = value.toString().split("\t");
        int age = Integer.parseInt(str[2]);

        if (numPartitions == 0) {
            return 0;
        }

        if (age <= 20) {
            return 0;
        } else if (age > 20 && age <= 30) {
            return 1 % numPartitions;
        } else {
            return 2 % numPartitions;
        }

    }
}

// End pPartitioner.java
