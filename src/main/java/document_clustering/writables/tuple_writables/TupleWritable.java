package document_clustering.writables.tuple_writables;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by edwardlol on 2016/12/1.
 */
abstract class TupleWritable<K extends Writable, V extends Writable> implements WritableComparable<TupleWritable> {
    //~ Instance fields --------------------------------------------------------

    K left;
    V right;

    //~ Constructors -----------------------------------------------------------

    TupleWritable() {

    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null || this.getClass() != that.getClass()) {
            return false;
        }
        TupleWritable guest = (TupleWritable) that;
        return this.left.equals(guest.left) && this.right.equals(guest.right);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.left.readFields(in);
        this.right.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.left.write(out);
        this.right.write(out);
    }

    public Writable getLeft() {
        return this.left;
    }

    public Writable getRight() {
        return this.right;
    }

    @Override
    public String toString() {
        return "(" + this.left + ',' + this.right + ')';
    }

}

// End TupleWritable.java
