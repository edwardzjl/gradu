package document_clustering.linkback.step1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * the key used in {@link Step1Mapper}
 * <p>
 * Created by edwardlol on 2016/12/26.
 */
public class Step1KeyWritable implements WritableComparable<Step1KeyWritable> {
    //~ Instance fields --------------------------------------------------------

    /**
     * id in mst
     */
    private IntWritable joinKey = new IntWritable();

    /**
     * secondary sort field
     * 1 = group_id, 2 = content
     */
    private IntWritable tag = new IntWritable();

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

    /**
     * main method, first sort by the joinKey, then keys with
     * the same joinKey value will have a secondary sort
     * on the value of the tag field, ensuring the order we want.
     *
     * @param step1KeyWritable another
     * @return
     */
    @Override
    public int compareTo(Step1KeyWritable step1KeyWritable) {
        int compareValue = this.joinKey.compareTo(step1KeyWritable.getJoinKey());
        if (compareValue == 0) {
            compareValue = this.tag.compareTo(step1KeyWritable.getTag());
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
