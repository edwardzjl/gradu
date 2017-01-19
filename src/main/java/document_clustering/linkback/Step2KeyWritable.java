package document_clustering.linkback;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/26.
 */
public class Step2KeyWritable implements WritableComparable<Step2KeyWritable> {
    //~ Instance fields --------------------------------------------------------

    /**
     * entry_id@@g_no
     */
    private Text joinKey = new Text();

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
    public int compareTo(Step2KeyWritable taggedKey) {
        int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
        if (compareValue == 0) {
            compareValue = this.tag.compareTo(taggedKey.getTag());
        }
        return compareValue;
    }

    public Text getJoinKey() {
        return this.joinKey;
    }

    public IntWritable getTag() {
        return this.tag;
    }

    public void set(String key, int tag) {
        this.joinKey.set(key);
        this.tag.set(tag);
    }
}

// End Step1KeyWritable.java
