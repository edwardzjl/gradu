package document_clustering.writables;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * a writable implementation for List<Long>
 * <p>
 * Created by edwardlol on 2016/11/26.
 */
public class LongListWritable extends LongCollectionWritable {
    //~ Constructors -----------------------------------------------------------

    public LongListWritable() {
        super();
    }

    public LongListWritable(List<Long> items) {
        super(items);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public void readFields(DataInput in) throws IOException {
        if (this.items != null) {
            this.items.clear();
        } else {
            this.items = new ArrayList<>();
        }

        int count = in.readInt();
        while (count-- > 0) {
            this.items.add(in.readLong());
        }
    }
}

// End LongListWritable.java
