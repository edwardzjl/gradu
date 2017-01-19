package document_clustering.writables.tuple_writables;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by edwardlol on 2016/12/1.
 */
public class TextDoubleTupleWritable extends TupleWritable<Text, DoubleWritable> {
    //~ Constructors -----------------------------------------------------------

    public TextDoubleTupleWritable() {
        this.left = new Text();
        this.right = new DoubleWritable();
    }

    public TextDoubleTupleWritable(String left, double right) {
        if (this.left == null) {
            this.left = new Text();
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
        TextDoubleTupleWritable guest = (TextDoubleTupleWritable) that;

        if (this.right.compareTo(guest.right) == 0) {
            return this.left.compareTo(guest.left);
        }
        return guest.right.compareTo(this.right);
    }

    public void set(Text left, DoubleWritable right) {
        this.left.set(left.toString());
        this.right.set(right.get());
    }

    public void set(String key, double tf_idf) {
        this.left.set(key);
        this.right.set(tf_idf);
    }

    public String getLeftValue() {
        return this.left.toString();
    }

    public Double getRightValue() {
        return this.right.get();
    }
}

// End TextDoubleTupleWritable.java
