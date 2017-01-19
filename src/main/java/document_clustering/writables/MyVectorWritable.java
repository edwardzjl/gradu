package document_clustering.writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Created by edwardlol on 2016/12/1.
 */
public class MyVectorWritable implements Writable {
    //~ Static fields/initializers ---------------------------------------------

    //~ Instance fields --------------------------------------------------------

    private double[] vector;

    //~ Constructors -----------------------------------------------------------

    public MyVectorWritable() {

    }

    public MyVectorWritable(double[] vector) {
        this.vector = vector;
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        this.vector = new double[count];
        for (int i = 0; i < count; i++) {
            this.vector[i] = in.readDouble();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.vector.length);
        for (double i : this.vector) {
            out.writeDouble(i);
        }
    }

    public double[] getVector() {
        return this.vector;
    }

    public void setVector(double[] vector) {
        this.vector = vector;
    }

    @Override
    public String toString() {
        if (this.vector == null)
            return "[]";

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('[');
        for (int i = 0; i < this.vector.length; i++) {
            stringBuilder.append(this.vector[i]);
            if (i < this.vector.length - 1) {
                stringBuilder.append(",");
            }
        }
        return stringBuilder.append(']').toString();
    }
}

// End MyVectorWritable.java
