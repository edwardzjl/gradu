package document_clustering.writables.tuple_writables;

import org.apache.hadoop.io.Text;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class TextTextTupleWritable extends TupleWritable<Text, Text> {
    //~ Constructors -----------------------------------------------------------

    public TextTextTupleWritable() {
        this.left = new Text();
        this.right = new Text();
    }

    public TextTextTupleWritable(String left, String right) {
        if (this.left == null) {
            this.left = new Text();
        }
        if (this.right == null) {
            this.right = new Text();
        }
        this.left.set(left);
        this.right.set(right);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public boolean equals(Object that) {
        if (!(that instanceof TextTextTupleWritable)) {
            return false;
        }
        TextTextTupleWritable guest = (TextTextTupleWritable) that;
        return (this.left.toString().equals(guest.left.toString())
                && this.right.toString().equals(guest.right.toString()))
                || (this.left.toString().equals(guest.right.toString())
                && this.right.toString().equals(guest.right.toString()));
    }

    @Override
    public int compareTo(TupleWritable that) {
        TextTextTupleWritable guest = (TextTextTupleWritable) that;

        if (this.left.compareTo(guest.left) == 0) {
            return this.right.compareTo(guest.right);
        }
        return this.left.compareTo(guest.left);
    }

    public void set(Text left, Text right) {
        this.left.set(left.toString());
        this.right.set(right.toString());
    }

    public void set(String left, String right) {
        this.left.set(left);
        this.right.set(right);
    }

    public String getLeftValue() {
        return this.left.toString();
    }

    public String getRightValue() {
        return this.right.toString();
    }

    @Override
    public String toString() {
        return "(" + this.left.toString() + "," + this.right.toString() + ")";
    }
}

// End TextIntTupleWritable.java
