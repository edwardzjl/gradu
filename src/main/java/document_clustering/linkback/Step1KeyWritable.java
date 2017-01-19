package document_clustering.linkback;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/26.
 */
public class Step1KeyWritable implements WritableComparable<Step1KeyWritable> {
    //~ Instance fields --------------------------------------------------------

    /**
     * id in mst
     */
    private IntWritable joinKey = new IntWritable();

    /**
     * 1 = group_id, 2 = content
     */
    private IntWritable tag = new IntWritable();

    //~ Constructors -----------------------------------------------------------

    //~ Methods ----------------------------------------------------------------

    @Override
    public void readFields(DataInput in) throws IOException {
        this.joinKey.readFields(in);
        this.tag.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.joinKey.write(out);
        this.tag.write(out);
    }

    @Override
    public int compareTo(Step1KeyWritable taggedKey) {
        int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
        if (compareValue == 0) {
            compareValue = this.tag.compareTo(taggedKey.getTag());
        }
        return compareValue;
    }

    public IntWritable getJoinKey() {
        return this.joinKey;
    }

    public IntWritable getTag() {
        return this.tag;
    }

    public void set(int key, int tag) {
        this.joinKey.set(key);
        this.tag.set(tag);
    }
}

// End Step1KeyWritable.java
