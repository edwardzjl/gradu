package document_clustering.linkback.step1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * {@link Step1KeyWritable}s with the same joinKey will be grouped together in the reducer
 * <p>
 * Created by edwardlol on 2016/12/26.
 */
public class TaggedJoiningGroupingComparator extends WritableComparator {
    //~ Constructors -----------------------------------------------------------

    public TaggedJoiningGroupingComparator() {
        super(Step1KeyWritable.class, true);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Step1KeyWritable taggedKey1 = (Step1KeyWritable) a;
        Step1KeyWritable taggedKey2 = (Step1KeyWritable) b;
        return taggedKey1.getJoinKey().compareTo(taggedKey2.getJoinKey());
    }
}

// End TaggedJoiningGroupingComparator.java
