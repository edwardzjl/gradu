package document_clustering.writables;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * a writable implementation for Set<Long>
 * <p>
 * Created by edwardlol on 2016/11/26.
 */
public class LongSetWritable extends LongCollectionWritable {
    //~ Constructors -----------------------------------------------------------

    public LongSetWritable() {
        super();
    }

    public LongSetWritable(Set<Long> items) {
        super(items);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public void readFields(DataInput in) throws IOException {
        if (this.items != null) {
            this.items.clear();
        } else {
            this.items = new HashSet<>();
        }

        int count = in.readInt();
        while (count-- > 0) {
            this.items.add(in.readLong());
        }
    }
}

// End LongSetWritable.java
