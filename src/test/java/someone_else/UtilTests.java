package someone_else;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import com.huaban.analysis.jieba.WordDictionary;
import document_clustering.util.CollectionUtil;
import document_clustering.util.JiebaFactory;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.synonym.SynonymFilterFactory;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.FilesystemResourceLoader;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by edwardlol on 2016/12/4.
 */
public class UtilTests {
    private Map<String, Set<Integer>> aaa = new HashMap<>();

    private int splitNum = 4;

    //~ Methods ----------------------------------------------------------------

    @Test
    public void sepTest() {
        String str = "欢迎使用ansj_seg,(ansj中文分词)在这里如果你遇到什么问题都可以联系我.我一定尽我所能.帮助大家.ansj_seg更快,更准,更自由!" ;
        System.out.println(ToAnalysis.parse(str));
    }

    private static String format(double value) {
        return String.format("%.2f", value);
    }

    @Test
    public void doubleTest() {
        System.out.println(format(1.254));
    }


    @Test
    public void wordTest() {
        try {
            String testInput = "其实 hankcs 似 好人";
            Version ver = Version.LUCENE_46;
            Map<String, String> filterArgs = new HashMap<>();
            filterArgs.put("luceneMatchVersion", ver.toString());
            filterArgs.put("synonyms", "./data/synonyms.dic");
            filterArgs.put("expand", "true");
            SynonymFilterFactory factory = new SynonymFilterFactory(filterArgs);
            factory.inform(new FilesystemResourceLoader());
            WhitespaceAnalyzer whitespaceAnalyzer = new WhitespaceAnalyzer(ver);
            TokenStream ts = factory.create(whitespaceAnalyzer.tokenStream("someField", testInput));
            displayTokens(ts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void displayTokens(TokenStream ts) throws IOException {
        CharTermAttribute termAttr = ts.addAttribute(CharTermAttribute.class);
//        OffsetAttribute offsetAttribute = ts.addAttribute(OffsetAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
            String token = termAttr.toString();
            System.out.print(token + " ");
        }
        System.out.println();
        ts.end();
        ts.close();
    }

    @Test
    public void jiebaTest2() {
        String test = "乘用车（1.6L）  221(几内亚)     乘用车（1.6L";
        JiebaFactory factory = JiebaFactory.getInstance("./dicts");
        List<String> terms = factory.seperate(test, JiebaSegmenter.SegMode.SEARCH);
        for (String term : terms) {
            System.out.println(term);
        }
    }

    @Test
    public void reTest() {
        String test = " 乘用车";
        String regex =
                "^[\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×\\s]+$|" +
                        "^\\s*\\d+\\.\\d+\\s*$|" +
                        "^\\s*[a-z牌型与也且的了于无不非个为之但人位全≥≦☆丨]\\s*$|" +
                        "^\\s*\\d+(?:|cc|ml)\\s*$|" +
                        "^\\s*car\\s*$|" +
                        "^\\s*乘用车\\s*$|" +
                        "^\\s*丰田\\s*$|" +
                        "^\\s*人座\\s*$|" +
                        "^\\s*休闲\\s*$|" +
                        "^\\s*kw\\s*$";
        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(test);
        if (matcher.find()) {
            System.out.println("hehe");
        }
    }


    @Test
    public void test() {
        double sim = 0.13494862701608235 * 0.13290819761474063 + 0.15588890374080677 * 0.15353185639939318
                + 0.0452014362881258 * 0.04451798850785179 + 0.03842325003006238 * 0.03784228873545729
                + 0.23062433385877734 * 0.22713728340205638 + 0.4089628510394415 * 0.402779314061196
                + 0.22028055145664022 * 0.216949899462068 + 0.21721311619696895 * 0.2139288439635617
                + 0.21349287870341022 * 0.21026485663074693;
        System.out.println(sim);
    }

    @Test
    public void splittest() {
        int containers = 0;
        for (int i = 0; i < splitNum; i++) {
            String ii = String.valueOf(i) + i;
            for (int j = i + 1; j < splitNum; j++) {
                String ij = String.valueOf(i) + j;
                String jj = String.valueOf(j) + j;

                CollectionUtil.updateSetMap(aaa, ii, containers);
                CollectionUtil.updateSetMap(aaa, ij, containers);
                CollectionUtil.updateSetMap(aaa, jj, containers);

                containers++;
            }
        }
        for (Map.Entry<String, Set<Integer>> entry : aaa.entrySet()) {
            Set<Integer> set = entry.getValue();
            System.out.print(entry.getKey() + ":");
            for (int container : set) {
                System.out.print(container + " ");
            }
            System.out.println();

        }
    }

    @Test
    public void normTest() {
        double[] vec1 = new double[3];
        double[] vec2 = new double[3];

        vec1[0] = 1;
        vec1[1] = 3;
        vec1[2] = 5;

        vec2[0] = 2;
        vec2[1] = 4;
        vec2[2] = 6;

        RandomAccessSparseVector vector1 = new RandomAccessSparseVector(3);
        RandomAccessSparseVector vector2 = new RandomAccessSparseVector(3);
        for (int i = 0; i < vec1.length; i++) {
            vector1.setQuick(i, vec1[i]);
            vector2.setQuick(i, vec2[i]);
        }
        CosineDistanceMeasure cdm = new CosineDistanceMeasure();

        System.out.println(cdm.distance(vector1, vector2));

        double sum = 0.0d;
        for (int i = 0; i < vec1.length; i++) {
            sum += Math.pow(vec1[i], 2);
        }
        sum = Math.sqrt(sum);
        for (int i = 0; i < vec1.length; i++) {
            vec1[i] /= sum;
        }
        sum = 0.0d;
        for (int i = 0; i < vec2.length; i++) {
            sum += Math.pow(vec2[i], 2);
        }
        sum = Math.sqrt(sum);
        for (int i = 0; i < vec2.length; i++) {
            vec2[i] /= sum;
        }
        for (int i = 0; i < vec1.length; i++) {
            vector1.setQuick(i, vec1[i]);
            vector2.setQuick(i, vec2[i]);
        }
        System.out.println(cdm.distance(vector1, vector2));
    }


    @Test
    public void jiebaTest() {
        String sentence = "美瑟达研磨咖啡粉(意式浓缩易理包)MESETA FORTE@@非速溶 已浸除咖啡碱且已焙炒|阿拉比卡和罗布斯塔咖啡豆|咖啡粉 咖啡豆经培炒后研磨成粉|美瑟达MESETA牌";
        String sentence2 = "陆地巡洋舰霸道越野车成套散件@@GRJ120L-GKAGKC 3956CC VX FQ 1859.6千克/套（06年税号）";
        JiebaSegmenter segment0 = new JiebaSegmenter();
        System.out.println(sentence2);
        System.out.println("--------------------------------------------------------------------");
        System.out.println("without dict:");
        List<SegToken> tokens = segment0.process(sentence2, JiebaSegmenter.SegMode.SEARCH);
        tokens.forEach(token -> {
            System.out.print(token.word + " ");
        });
        System.out.println();


        WordDictionary wordDictionary = WordDictionary.getInstance();
        wordDictionary.init(Paths.get("dicts"));
        JiebaSegmenter segment = new JiebaSegmenter();

        System.out.println("with dict:");
        List<SegToken> tokens2 = segment.process(sentence2, JiebaSegmenter.SegMode.SEARCH);
        tokens2.forEach(token -> {
            System.out.print(token.word + " ");
        });
        System.out.println();
    }


    @Test
    public void eucSim() {
        String _vector1 = "174:0.36197029750794574,3007:0.21007112808028006,1386:0.21340515611859084,1383:0.3242105991798923,3773:0.15783786905280167,2310:0.14236552147596176,2392:0.1374839088421808,769:0.342791492677827,1206:0.25467443667900574,2738:0.11541308430338042,4375:0.10053684754909976,2311:0.3559055122942307,3619:0.08500971064362904,3410:0.13501250041305013";
        String _vector2 = "3619:0.11592233269585779,1206:0.34728332274409873,3634:0.22546504300327638,3792:0.15791908566734128,1386:0.2910070310708057,3007:0.2864606292003819,4375:0.13709570120331788,3773:0.10761672889963751,2310:0.38826960402535027,1383:0.442105362518035";
        printEucDist(_vector1, _vector2);

        String _vector3 = "2732:0.5006090858238013,567:0.8237180998914855,1760:0.9546167758735077,3773:0.16911200255657322,1345:0.48029844456943804,588:0.7565747242849519,3411:0.19176834141081403";
        String _vector4 = "588:0.8826705116657771,1760:1.1137195718524255,3773:0.1972973363160021,2732:0.5840439334611014,567:0.9610044498733998,1345:0.560348185331011";
        printEucDist(_vector3, _vector4);

        String _vector5 = "1600:0.6565697269867857,4125:0.16144365956149656,3411:0.29830630886126625,3792:0.19301221581563932,3773:0.1315315575440014,2426:0.2246697717672648,1969:0.5636532798533836,1968:0.5636532798533836";
        String _vector6 = "1969:0.6341099398350567,3792:0.21713874279259424,3411:0.33559459746892456,1600:0.738640942860134,1968:0.6341099398350567,3773:0.14797300223700158,4125:0.18162411700668363";
        printEucDist(_vector5, _vector6);

        String _vector7 = "2443:0.546525426539244,3678:0.4007269388763363,2320:0.4711078217494833,193:0.5262674757843601,940:0.49233618138978436,3773:0.1315315575440014,2434:0.33168213046510253,1676:0.31747842251955977,585:0.29488821888780525";
        String _vector8 = "940:0.7385042720846765,193:0.78940121367654,585:0.4423323283317079,2443:0.8197881398088659,1676:0.4762176337793396,2320:0.7066617326242249";
        printEucDist(_vector7, _vector8);

        String _vector9 = "1206:0.38201165501850864,1957:0.5141872390167406,3411:0.13423783898756983,2079:0.5989170250554608,1972:0.5040089695857464,1588:0.4503784986790487,769:0.5141872390167406,1383:0.4863158987698385,2087:0.5909127542881073,1386:0.3201077341778863";
        String _vector10 = "2087:0.5371934129891884,1972:0.4581899723506785,1957:0.4674429445606732,769:0.4674429445606732,3411:0.12203439907960893,1206:0.34728332274409873,1588:0.4094349987991352,856:0.43501794965715207,1639:0.6704967828795,1386:0.2910070310708057,2079:0.5444700227776917";
        printEucDist(_vector9, _vector10);

        String _vector11 = "1201:0.5967626231089261,2392:0.15863527943328556,3410:0.15578365432275015,3185:0.34220963150826617,2310:0.1642679093953405,1016:0.6047357104727086,4375:0.1160040548643459,2439:0.4545482725293133,2440:0.4545482725293133,2738:0.1331689434269774,3411:0.10325987614428449";
        String _vector12 = "2310:0.1779569018449522,2392:0.17185488605272603,3410:0.16876562551631263,1201:0.6464928417013366,3411:0.2237297316459497,4375:0.1256710594363747,1016:0.32756517650605044,2433:0.16591412907378422,583:0.22521964810104944,2738:0.1442663553792255";
        printEucDist(_vector11, _vector12);

        String _vector13 = "2067:0.3063286634226971,2732:0.4380329500958261,3411:0.16779729873446228,528:0.7846065403757987,1345:0.4202611389982583,2924:0.8580798734885636,2923:0.8580798734885636,989:0.5115062832085909";
        String _vector14 = "528:3.1384261615031948,2067:1.2253146536907884";
        printEucDist(_vector13, _vector14);

        String _vector15 = "4375:0.1256710594363747,2310:0.1779569018449522,3619:0.1062621383045363,4125:0.12108274467112241,2113:0.19614829011597157,3533:0.6484108099818886,1715:0.23311025817286063,3634:0.20667628941967,3532:0.6484108099818886,3792:0.1447591618617295,3773:0.09864866815800105,921:0.3785209389681903";
        String _vector16 = "1311:0.24644259853545236,921:0.32444651911559164,1001:0.2854814347046002,4375:0.10771805094546402,1987:0.17840899936033397,1715:0.19980879271959484,2113:0.16812710581368992,3619:0.09108183283245969,2310:0.15253448729567332,3634:0.17715110521686,1990:0.17697320083739917,3792:0.12407928159576813,4125:0.10378520971810493,1676:0.20409327161971696";
        printEucDist(_vector15, _vector16);

    }


    private void printEucDist(String _vector1, String _vector2) {
        EuclideanDistanceMeasure dis = new EuclideanDistanceMeasure();
        RandomAccessSparseVector vector1 = makeVector(_vector1);
        RandomAccessSparseVector vector2 = makeVector(_vector2);
        System.out.println(dis.distance(vector1, vector2));
    }

    private RandomAccessSparseVector makeVector(String vString) {
        RandomAccessSparseVector vector = new RandomAccessSparseVector(4789);
        String[] _contents1 = vString.split(",");
        for (String content1 : _contents1) {
            String[] tmp = content1.split(":");
            vector.setQuick(Integer.parseInt(tmp[0]), Double.parseDouble(tmp[1]));
        }
        return vector;
    }
}

// End UtilTests.java
