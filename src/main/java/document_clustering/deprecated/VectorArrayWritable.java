package document_clustering.deprecated;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.mahout.math.VectorWritable;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class VectorArrayWritable extends ArrayWritable {
    //~ Constructors -----------------------------------------------------------

    public VectorArrayWritable() {
        super(VectorWritable.class);
    }

    public VectorArrayWritable(VectorWritable[] values) {
        super(VectorWritable.class, values);
    }
}

// End VectorArrayWritable.java
