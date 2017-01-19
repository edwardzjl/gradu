package document_clustering.writables.tuple_writables;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Created by edwardlol on 2016/12/1.
 */
public class IntDoubleTupleWritable extends TupleWritable<IntWritable, DoubleWritable> {
    //~ Constructors -----------------------------------------------------------

    public IntDoubleTupleWritable() {
        this.left = new IntWritable();
        this.right = new DoubleWritable();
    }

    public IntDoubleTupleWritable(int left, double right) {
        if (this.left == null) {
            this.left = new IntWritable();
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
        IntDoubleTupleWritable guest = (IntDoubleTupleWritable) that;
        return this.left.compareTo(guest.left);
    }

    public void set(IntWritable left, DoubleWritable right) {
        this.left.set(left.get());
        this.right.set(right.get());
    }

    public void set(int key, double tf_idf) {
        this.left.set(key);
        this.right.set(tf_idf);
    }

    public Integer getLeftValue() {
        return this.left.get();
    }

    public Double getRightValue() {
        return this.right.get();
    }
}

// End IntDoubleTupleWritable.java
