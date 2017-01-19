package document_clustering.writables.tuple_writables;

import document_clustering.writables.MyVectorWritable;

/**
 * Created by edwardlol on 2016/12/2.
 */
public class VectorVectorTupleWritable extends TupleWritable<MyVectorWritable, MyVectorWritable> {

    //~ Constructors -----------------------------------------------------------

    public VectorVectorTupleWritable() {
        this.left = new MyVectorWritable();
        this.right = new MyVectorWritable();
    }

    public VectorVectorTupleWritable(double[] left, double[] right) {
        if (this.left == null) {
            this.left = new MyVectorWritable();
        }
        if (this.right == null) {
            this.right = new MyVectorWritable();
        }
        this.left.setVector(left);
        this.right.setVector(right);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public int compareTo(TupleWritable o) {
        return 0;
    }

    public void set(MyVectorWritable left, MyVectorWritable right) {
        this.left.setVector(left.getVector());
        this.right.setVector(right.getVector());
    }

    public void set(double[] left, double[] right) {
        this.left.setVector(left);
        this.right.setVector(right);
    }

    public double[] getLeftValue() {
        return this.left.getVector();
    }

    public double[] getRightValue() {
        return this.right.getVector();
    }
}

// End VectorVectorTupleWritable.java
