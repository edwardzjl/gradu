package clustering;

import document_clustering.inverted_index.FilterDriver;
import document_clustering.inverted_index.InvertedIndexDriver;
import org.junit.Test;

/**
 * Created by edwardlol on 17-2-22.
 */
public class InvertedIndexTests {

    /**
     * read the big document file and calculate the inverted index
     * the inverted index is formatted like: term \t [document_id...]
     * the document id is represented as line number in the big document file
     */
    @Test
    public void invertedIndexTest() {
        InvertedIndexDriver driver = new InvertedIndexDriver();
        String[] args = new String[2];
        args[0] = "/final/tf_idf/0901/result";
        args[1] = "/final/iindex/0901_id";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void filteredInvertedIndexTest() {
        FilterDriver driver = new FilterDriver();
        String[] args = new String[2];
        args[0] = "/final/bench/iindex";
        args[1] = "/final/bench/filtered_iindex";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
