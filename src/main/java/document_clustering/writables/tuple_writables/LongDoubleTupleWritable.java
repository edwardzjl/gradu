package document_clustering.writables.tuple_writables;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Created by edwardlol on 2016/12/1.
 */
public class LongDoubleTupleWritable extends TupleWritable<LongWritable, DoubleWritable> {
    //~ Constructors -----------------------------------------------------------

    public LongDoubleTupleWritable() {
        this.left = new LongWritable();
        this.right = new DoubleWritable();
    }

    public LongDoubleTupleWritable(long left, double right) {
        if (this.left == null) {
            this.left = new LongWritable();
        }
        if (this.right == null) {
            this.right = new DoubleWritable();
        }
        this.left.set(left);
        this.right.set(right);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public int compareTo(TupleWritable that) {
        LongDoubleTupleWritable guest = (LongDoubleTupleWritable) that;

        if (this.right.compareTo(guest.right) != 0) {
            return guest.right.compareTo(this.right);
        } else {
            return this.left.compareTo(guest.left);
        }
    }

    public void set(LongWritable left, DoubleWritable right) {
        this.left.set(left.get());
        this.right.set(right.get());
    }

    public void set(long key, double tf_idf) {
        this.left.set(key);
        this.right.set(tf_idf);
    }

    public Long getLeftValue() {
        return this.left.get();
    }

    public Double getRightValue() {
        return this.right.get();
    }
}

// End LongDoubleTupleWritable.java
