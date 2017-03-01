package word_sep;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import com.huaban.analysis.jieba.WordDictionary;
import document_clustering.util.JiebaFactory;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by edwardlol on 17-2-21.
 */
public class SepTest {


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
    public void ansjTest1() {
        String str = "欢迎使用ansj_seg,(ansj中文分词)在这里如果你遇到什么问题都可以联系我.我一定尽我所能.帮助大家.ansj_seg更快,更准,更自由!";
        System.out.println(ToAnalysis.parse(str));
    }

    @Test
    public void ansjTest2() {
        String str = "洁面仪配合洁面深层清洁毛孔 清洁鼻孔面膜碎觉使劲挤才能出一点点皱纹 脸颊毛孔修复的看不见啦 草莓鼻历史遗留问题没辙 脸和脖子差不多颜色的皮肤才是健康的 长期使用安全健康的比同龄人显小五到十岁 28岁的妹子看看你们的鱼尾纹";
        String str2 = "未烘焙的咖啡豆ESTATE COFFEE SAN AGUSTIN@@豆,未浸除咖啡碱,未焙炒|阿拉比卡|咖啡豆|OPERADORA SANTA CLARA|69KG/袋*25袋";
        for (Term term : NlpAnalysis.parse(str2)) {
            System.out.println(term + " ");
            if (term.getSynonyms() != null) {
                for (String syn : term.getSynonyms()) {
                    System.out.println(syn);
                }
            }
        }
//        System.out.println(NlpAnalysis.parse(str));
    }


    @Test
    public void jiebaTest1() {
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
    public void jiebaTest2() {
        String test = "乘用车（1.6L）  221(几内亚)     乘用车（1.6L";
        JiebaFactory factory = JiebaFactory.getInstance("./dicts");
        List<String> terms = factory.seperate(test, JiebaSegmenter.SegMode.SEARCH);
        for (String term : terms) {
            System.out.println(term);
        }
    }
}
