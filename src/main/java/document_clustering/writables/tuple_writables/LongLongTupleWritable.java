package document_clustering.writables.tuple_writables;

import org.apache.hadoop.io.LongWritable;

/**
 * Created by edwardlol on 2016/12/1.
 */
public class LongLongTupleWritable extends TupleWritable<LongWritable, LongWritable> {
    //~ Constructors -----------------------------------------------------------

    public LongLongTupleWritable() {
        this.left = new LongWritable();
        this.right = new LongWritable();
    }

    public LongLongTupleWritable(long left, long right) {
        if (this.left == null) {
            this.left = new LongWritable();
        }
        if (this.right == null) {
            this.right = new LongWritable();
        }
        this.left.set(left);
        this.right.set(right);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public int compareTo(TupleWritable that) {
        LongLongTupleWritable guest = (LongLongTupleWritable) that;

        if (this.left.compareTo(guest.left) == 0) {
            return this.right.compareTo(guest.right);
        }
        return this.left.compareTo(guest.left);
    }

    public void set(LongWritable left, LongWritable right) {
        this.left.set(left.get());
        this.right.set(right.get());
    }

    public void set(long key, long tf_idf) {
        this.left.set(key);
        this.right.set(tf_idf);
    }

    public long getLeftValue() {
        return this.left.get();
    }

    public long getRightValue() {
        return this.right.get();
    }
}

// End LongLongTupleWritable.java
