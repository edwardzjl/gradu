package document_clustering.writables.tuple_writables;

import org.apache.hadoop.io.IntWritable;

/**
 * Created by edwardlol on 2016/12/18.
 */
public class IntIntTupleWritable extends TupleWritable<IntWritable, IntWritable> {

    //~ Constructors -----------------------------------------------------------

    public IntIntTupleWritable() {
        this.left = new IntWritable();
        this.right = new IntWritable();
    }

    public IntIntTupleWritable(int left, int right) {
        if (this.left == null) {
            this.left = new IntWritable();
        }
        if (this.right == null) {
            this.right = new IntWritable();
        }
        this.left.set(left);
        this.right.set(right);
    }
    //~ Methods ----------------------------------------------------------------


    @Override
    public boolean equals(Object that) {
        if (!(that instanceof IntIntTupleWritable)) {
            return false;
        }
        IntIntTupleWritable guest = (IntIntTupleWritable) that;
        return (this.left.equals(guest.left) && this.right.equals(guest.right));
    }

    @Override
    public int compareTo(TupleWritable that) {
        IntIntTupleWritable guest = (IntIntTupleWritable) that;

        int result = this.left.compareTo(guest.left);
        if (result == 0) {
            return this.right.compareTo(guest.right);
        }
        return result;
    }


    public void set(IntWritable left, IntWritable right) {
        this.left.set(left.get());
        this.right.set(right.get());
    }

    public void set(int left, int right) {
        this.left.set(left);
        this.right.set(right);
    }

    public int getLeftValue() {
        return this.left.get();
    }

    public int getRightValue() {
        return this.right.get();
    }

    @Override
    public String toString() {
        return "(" + this.left.toString() + "," + this.right.toString() + ")";
    }
}

// End IntIntTupleWritable.java
