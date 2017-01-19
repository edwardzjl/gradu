package document_clustering.writables;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class SimilarityMidOutWritable implements Writable {
    //~ Instance fields --------------------------------------------------------

    private LongWritable key;
    private VectorWritable value;

    //~ Constructors -----------------------------------------------------------

    public SimilarityMidOutWritable() {
        this.key = new LongWritable();
        this.value = new VectorWritable();
    }

    public SimilarityMidOutWritable(LongWritable key, VectorWritable value) {
        this.key = new LongWritable(key.get());
        this.value = new VectorWritable(value.get());
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public void readFields(DataInput in) throws IOException {
        this.key.readFields(in);
        this.value.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.key.write(out);
        this.value.write(out);
    }

    public LongWritable getKey() {
        return this.key;
    }

    public VectorWritable getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.key.toString() + "\n" + this.value.toString();
    }
}

// End SimilarityMidOutWritable.java
