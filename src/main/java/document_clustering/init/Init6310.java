package document_clustering.init;

import com.huaban.analysis.jieba.JiebaSegmenter;
import document_clustering.util.CollectionUtil;
import document_clustering.util.JiebaFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by edwardlol on 2016/12/22.
 */
public class Init6310 {
    //~ Static fields/initializers ---------------------------------------------

    private static JiebaFactory jieba;

    //~ Methods ----------------------------------------------------------------
    public static void pre6310() {
        Map<String, Integer> entry_ids = new HashMap<>();

        try (FileReader fr = new FileReader("./datasets/6310.csv");
             BufferedReader br = new BufferedReader(fr);
             FileWriter fw = new FileWriter("./results/6310pre");
             BufferedWriter bw = new BufferedWriter(fw)) {
            String line = br.readLine();
            String[] _contents = line.split(",");
            bw.append("id").append(',').append(_contents[12]).append(',')
                    .append(_contents[13]).append(',').append(_contents[14]).append(',')
                    .append(_contents[19]).append(',').append(_contents[21]).append('\n');
            bw.flush();
            line = br.readLine();
            int cnt = 0;
            while (line != null) {
                String[] contents = line.split(",");

                if (contents.length == 24) {
                    String entry_id = contents[0];
                    CollectionUtil.updateCountMap(entry_ids, entry_id, 1);
                    int g_no = entry_ids.get(entry_id);

                    bw.append(entry_id).append("@@").append(String.valueOf(g_no)).append(',')
                            .append(contents[12]).append(',')
                            .append(contents[13]).append(',').append(contents[14]).append(',')
                            .append(contents[19]).append(',').append(contents[21]).append('\n');
                    bw.flush();
                } else {
                    cnt++;
                }
                line = br.readLine();
            }
            System.out.println(cnt);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void init6310() {
        jieba = JiebaFactory.getInstance("./dicts");
        Map<String, Integer> entry_ids = new HashMap<>();

        try (FileReader fr = new FileReader("./datasets/6310.csv");
             BufferedReader br = new BufferedReader(fr);
             FileWriter fw = new FileWriter("./results/6310");
             BufferedWriter bw = new BufferedWriter(fw)) {
            String line = br.readLine();
            line = br.readLine();
            while (line != null) {
                String[] contents = line.split(",");

                if (contents.length == 24) {
                    String entry_id = contents[0];
                    CollectionUtil.updateCountMap(entry_ids, entry_id, 1);
                    int g_no = entry_ids.get(entry_id);

                    String g_name = contents[13];
                    String g_model = contents[14];

                    bw.append(entry_id).append("@@").append(String.valueOf(g_no)).append('\t');

                    List<String> nterms = jieba.seperate(g_name, JiebaSegmenter.SegMode.SEARCH);
                    Iterator<String> niterator = nterms.iterator();
                    while (niterator.hasNext()) {
                        bw.append(niterator.next());
                        if (niterator.hasNext()) {
                            bw.append(" ");
                        }
                    }
                    bw.append("##");
                    if (!g_model.equals("")) {
                        List<String> mterms = jieba.seperate(g_model, JiebaSegmenter.SegMode.SEARCH);
                        Iterator<String> miterator = mterms.iterator();
                        while (miterator.hasNext()) {
                            bw.append(miterator.next());
                            if (miterator.hasNext()) {
                                bw.append(" ");
                            }
                        }
                    }
                    bw.append("\n");
                    bw.flush();
                }
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void init63102() {
        jieba = JiebaFactory.getInstance("./dicts");

        try (FileReader fr = new FileReader("./results/6310pre");
             BufferedReader br = new BufferedReader(fr);
             FileWriter fw = new FileWriter("./results/6310");
             BufferedWriter bw = new BufferedWriter(fw)) {
            String line = br.readLine();
            line = br.readLine();
            while (line != null) {
                String[] contents = line.split(",");

                String id = contents[0];
                String g_name = contents[2];
                String g_model = contents[3];

                bw.append(id).append('\t');

                List<String> nterms = jieba.seperate(g_name, JiebaSegmenter.SegMode.SEARCH);
                Iterator<String> niterator = nterms.iterator();
                while (niterator.hasNext()) {
                    bw.append(niterator.next());
                    if (niterator.hasNext()) {
                        bw.append(' ');
                    }
                }
                bw.append("##");
                if (!g_model.equals("")) {
                    List<String> mterms = jieba.seperate(g_model, JiebaSegmenter.SegMode.SEARCH);
                    Iterator<String> miterator = mterms.iterator();
                    while (miterator.hasNext()) {
                        bw.append(miterator.next());
                        if (miterator.hasNext()) {
                            bw.append(' ');
                        }
                    }
                }
                bw.append("\n");
                bw.flush();

                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        pre6310();
//        init63102();
    }
}

// End Init6310.java
