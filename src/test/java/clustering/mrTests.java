package clustering;

import benchmark.tools.DuplicatorDriver;
import document_clustering.deprecated.MergeDriver;
import document_clustering.deprecated.OneNodeSimilarityDriver;
import document_clustering.deprecated.SplitedSimilarityDriver;
import document_clustering.deprecated.VectorDriver;
import document_clustering.linkback.LastStepDriver;
import document_clustering.linkback.ProcessClusterDriver;
import document_clustering.linkback.Step1Driver;
import document_clustering.linkback.Step2Driver;
import document_clustering.linkback.bas0901.Process0901Driver;
import document_clustering.linkback.bas8703.Process8703Driver;
import document_clustering.mst.MSTDriver;
import document_clustering.mst.MSTTestDriver2;
import document_clustering.simhash.SimHashDriver;
import document_clustering.similarity.ISimDriver;
import document_clustering.deprecated.InvertedSimilarityDriver;
import org.junit.Ignore;
import org.junit.Test;
import partition_test.pDriver;

/**
 * Created by edwardlol on 2016/11/25.
 */
public class mrTests {

    @Test
    public void dup() {
        DuplicatorDriver driver = new DuplicatorDriver();
        String[] args = new String[2];
        args[0] = "/user/edwardlol/final/todup/in";
        args[1] = "/user/edwardlol/final/todup/out";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // my mst
    @Test
    public void mstTest() {
        MSTDriver driver = new MSTDriver();
        String[] args = new String[3];
        args[0] = "/user/edwardlol/final/price/isim";
        args[1] = "/user/edwardlol/final/price/tf_idf/docCount";
        args[2] = "/user/edwardlol/final/price/mst";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void resultPre8703Test() {
        Process8703Driver driver = new Process8703Driver();
        String[] args = new String[2];
        args[0] = "/user/edwardlol/playground/bas";
        args[1] = "/user/edwardlol/playground/pro_bas";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void resultStep1Test() {
        Step1Driver driver = new Step1Driver();
        String[] args = new String[3];
        args[0] = "/user/edwardlol/playground/mst/0901";
        args[1] = "/user/edwardlol/playground/simhash/0901";
        args[2] = "/user/edwardlol/playground/step1/0901";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void resultPreClusterTest() {
        ProcessClusterDriver driver = new ProcessClusterDriver();
        String[] args = new String[3];
        args[0] = "/user/edwardlol/playground/step1/0901";
        args[1] = "/user/edwardlol/playground/step2/0901";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void resultStep2Test() {
        Step2Driver driver = new Step2Driver();
        String[] args = new String[3];
        args[0] = "/user/edwardlol/playground/step2/0901";
        args[1] = "/user/edwardlol/playground/pro_bas/0901";
        args[2] = "/user/edwardlol/playground/step3/0901";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void resultLastStepTest() {
        LastStepDriver driver = new LastStepDriver();
        String[] args = new String[2];
        args[0] = "/user/edwardlol/playground/step3/0901";
        args[1] = "/user/edwardlol/playground/finalresult/0901";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void resultPre0901Test() {
        Process0901Driver driver = new Process0901Driver();
        String[] args = new String[2];
        args[0] = "/user/edwardlol/playground/bas/0901";
        args[1] = "/user/edwardlol/playground/pro_bas/0901";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }







    // their
    @Test
    public void mstTest2() {
        MSTTestDriver2 driver = new MSTTestDriver2();
        String[] args = new String[3];
        args[0] = "/user/edwardlol/playground/sim";
        args[1] = "/user/edwardlol/playground/mst2";
        args[2] = "/user/edwardlol/playground/inverted_index/docCnt";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Deprecated
    @Ignore
    @Test
    public void partitionTest() {
        long starttime = System.currentTimeMillis();
        pDriver driver = new pDriver();
        String[] args = new String[2];
        args[0] = "/user/edwardlol/partition/in";
        args[1] = "/user/edwardlol/partition/out";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endtime = System.currentTimeMillis();
        System.out.println((endtime - starttime) / 1000);
    }

    @Deprecated
    @Ignore
    @Test
    public void vectorTest() {
        VectorDriver driver = new VectorDriver();
        String[] args = new String[3];
        args[0] = "/user/edwardlol/tf_idf/result";
        args[1] = "/user/edwardlol/invertedIndex";
        args[2] = "/user/edwardlol/vectors";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Deprecated
    @Ignore
    @Test
    public void invertedSimilarityTest() {
        InvertedSimilarityDriver driver = new InvertedSimilarityDriver();
        String[] args = new String[3];
        args[0] = "/user/edwardlol/debug/inverted_index";
        args[1] = "/user/edwardlol/debug/isim";
        args[2] = "1";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Deprecated
    @Ignore
    @Test
    public void oneNodeSimTest() {
        OneNodeSimilarityDriver driver = new OneNodeSimilarityDriver();
        String[] args = new String[2];
        args[0] = "/user/edwardlol/vectors/result";
        args[1] = "/user/edwardlol/similarity2";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Deprecated
    @Ignore
    @Test
    public void splitSimilarity() {
        SplitedSimilarityDriver driver = new SplitedSimilarityDriver();
        String[] args = new String[5];
        args[0] = "/user/edwardlol/playground/vectors";
        args[1] = "/user/edwardlol/playground/documentCount";
        args[2] = "/user/edwardlol/playground/splitedSimilarities_less";
        args[3] = "3";
        args[4] = "0.71";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Deprecated
    @Ignore
    @Test
    public void mergeSim() {
        long starttime = System.currentTimeMillis();
        MergeDriver driver = new MergeDriver();
        String[] args = new String[2];
        args[0] = "/user/edwardlol/playground/splitedSimilarities_less";
        args[1] = "/user/edwardlol/playground/mergedSimilarities_less";
        try {
            driver.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endtime = System.currentTimeMillis();
        System.err.println((endtime - starttime) / 1000);
    }

}
