package document_clustering.linkback;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by edwardlol on 2016/12/26.
 */
public class Step2GroupingComparator extends WritableComparator {
    //~ Constructors -----------------------------------------------------------

    public Step2GroupingComparator() {
        super(Step2KeyWritable.class, true);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Step2KeyWritable taggedKey1 = (Step2KeyWritable) a;
        Step2KeyWritable taggedKey2 = (Step2KeyWritable) b;
        return taggedKey1.getJoinKey().compareTo(taggedKey2.getJoinKey());
    }
}

// End TaggedJoiningGroupingComparator.java
