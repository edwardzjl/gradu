package document_clustering.writables.tuple_writables;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class TextIntTupleWritable extends TupleWritable<Text, IntWritable> {
    //~ Constructors -----------------------------------------------------------

    public TextIntTupleWritable() {
        this.left = new Text();
        this.right = new IntWritable();
    }

    public TextIntTupleWritable(String left, int right) {
        if (this.left == null) {
            this.left = new Text();
        }
        if (this.right == null) {
            this.right = new IntWritable();
        }
        this.left.set(left);
        this.right.set(right);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public int compareTo(TupleWritable that) {
        TextIntTupleWritable guest = (TextIntTupleWritable) that;

        if (this.right.compareTo(guest.right) == 0) {
            return this.left.compareTo(guest.left);
        }
        return guest.right.compareTo(this.right);
    }

    public void set(Text left, IntWritable right) {
        this.left.set(left.toString());
        this.right.set(right.get());
    }

    public void set(String key, int tf_idf) {
        this.left.set(key);
        this.right.set(tf_idf);
    }

    public String getLeftValue() {
        return this.left.toString();
    }

    public int getRightValue() {
        return this.right.get();
    }

}

// End TextIntTupleWritable.java
