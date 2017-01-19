package document_clustering.writables;

import org.apache.hadoop.io.WritableComparable;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by edwardlol on 2016/11/25.
 */
public abstract class CollectionWritable<T> implements WritableComparable<CollectionWritable> {
    //~ Instance fields --------------------------------------------------------

    Collection<T> items;

    //~ Constructors -----------------------------------------------------------

    CollectionWritable() {

    }

    CollectionWritable(Collection<T> items) {
        this.items = items;
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public int compareTo(CollectionWritable that) {
        if (this.items.equals(that.items)) {
            return 0;
        } else {
            // TODO: 2016/11/25 maybe i can do better here
            return this.items.size() >= that.items.size() ? 1 : -1;
        }
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null || this.getClass() != that.getClass()) {
            return false;
        }
        CollectionWritable guest = (CollectionWritable) that;
        return this.items.equals(guest.items);
    }

    @Override
    public int hashCode() {
        return this.items.hashCode();
    }

    @Override
    public String toString() {
        return this.items.toString();
    }

    public Collection<T> getItems() {
        return this.items;
    }

    public void set(Collection<T> items) {
        this.items = items;
    }

    public Iterator<T> iterator() {
        return this.items.iterator();
    }

    public void clear() {
        this.items.clear();
    }
}

// End CollectionWritable.java
