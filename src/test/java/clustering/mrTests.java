package clustering;

import benchmark.tools.DuplicatorDriver;
import document_clustering.linkback.LastStepDriver;
import document_clustering.linkback.ProcessClusterDriver;
import document_clustering.linkback.step1.Step1Driver;
import document_clustering.linkback.step2.Step2Driver;
import document_clustering.linkback.pre.Process0901Driver;
import document_clustering.linkback.pre.Process8703Driver;
import document_clustering.mst.deprecated.MSTTestDriver2;
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


}
