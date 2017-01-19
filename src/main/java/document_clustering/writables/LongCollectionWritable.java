package document_clustering.writables;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

/**
 * Created by edwardlol on 2016/11/26.
 */
abstract class LongCollectionWritable extends CollectionWritable<Long> {
    //~ Constructors -----------------------------------------------------------

    LongCollectionWritable() {
        super();
    }

    LongCollectionWritable(Collection<Long> items) {
        super(items);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.items.size());
        for (long i : this.items) {
            out.writeLong(i);
        }
    }
}

// End LongCollectionWritable.java
