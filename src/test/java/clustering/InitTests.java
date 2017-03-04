package clustering;

import document_clustering.init.Init0901Driver;
import document_clustering.init.Init8703Driver;
import org.junit.Test;

/**
 * Created by edwardlol on 17-2-22.
 */
public class InitTests {

    /**
     * read the defect lib csv file and get the contents in "description" field, each line as a document
     * then seperate the terms of each document, writables them in the format of id, term...
     */
    @Test
    public void init0901Test() {
        Init0901Driver driver = new Init0901Driver();
        String[] args = new String[2];
        args[0] = "/final/init/in/0901";
        args[1] = "/final/init/out/0901";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void init8703Test() {
        Init8703Driver driver = new Init8703Driver();
        String[] args = new String[2];
        args[0] = "/init/in";
        args[1] = "/init/out";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
