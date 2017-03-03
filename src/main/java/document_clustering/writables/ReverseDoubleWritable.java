package document_clustering.writables;

import org.apache.hadoop.io.DoubleWritable;

/**
 * extends {@link DoubleWritable} and reverse the sort order
 * <p>
 * Created by edwardlol on 2016/12/2.
 */
public class ReverseDoubleWritable extends DoubleWritable {
    //~ Methods ----------------------------------------------------------------

    @Override
    public int compareTo(DoubleWritable o) {
        return (super.get() < o.get() ? 1 : (super.get() == o.get() ? 0 : -1));
    }
}

// End ReverseDoubleWritable.java
