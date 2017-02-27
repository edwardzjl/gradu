package document_clustering.init;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import com.huaban.analysis.jieba.WordDictionary;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.DicAnalysis;

import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by edwardlol on 17-2-22.
 */
public class SepUtils {

    private static final Pattern stoppingPattern = Pattern.compile("^[\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×≥≦☆Ⅲ°≤丨\\s]+$");

    private static String stoppingRegex8703 = "^[\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×\\s]+$|" +
            "^\\s*\\d+\\.\\d+\\s*$|" +
            "^\\s*[a-zA-Z\\d]+\\s*$|" +
            "^\\s*[牌型与也且的了于无不非个为之但人位全≥≦☆Ⅲ°≤丨]\\s*$|" +
            "^\\s*\\d+(?:|cc|ml)\\s*$|" +
            "^\\s*car\\s*$|" +
            "^\\s*乘用车\\s*$|" +
            "^\\s*汽油\\s*$|" +
            "^\\s*人座\\s*$|" +
            "^\\s*休闲\\s*$|" +
            "^\\s*厂牌\\s*$|" +
            "^\\s*kw\\s*$";
    private static Pattern stoppingPattern8703 = Pattern.compile(stoppingRegex8703);

    private static JiebaSegmenter jiebaSegmenter;

    /**
     * @param stringBuilder
     * @param sentence
     * @return
     */
    static StringBuilder appendByAnsj(StringBuilder stringBuilder, String sentence) {
        for (Term term : DicAnalysis.parse(sentence)) {
            String word = term.getName();
            Matcher matcher = stoppingPattern.matcher(word);
            if (!matcher.find()) {
                stringBuilder.append(word).append(' ');
            }
        }
        try {
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        } catch (RuntimeException e) {
            System.out.println(stringBuilder.toString());
        }
        return stringBuilder;
    }

    /**
     * append the seperate result to the StringBuilder
     *
     * @param stringBuilder stringBuilder to append to
     * @param sentence      sentence to be seperated
     * @return stringBuilder
     */
    static StringBuilder appendByJieba(StringBuilder stringBuilder, String sentence) {
        for (SegToken term : jiebaSegmenter.process(sentence, JiebaSegmenter.SegMode.SEARCH)) {
            String word = term.word;
            Matcher matcher = stoppingPattern8703.matcher(word);
            if (!matcher.find()) {
                stringBuilder.append(term).append(' ');
            }
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return stringBuilder;
    }

    /**
     * @param dictPath
     */
    public static void setJiebaDict(String dictPath) {
        WordDictionary wordDictionary = WordDictionary.getInstance();
        wordDictionary.init(Paths.get(dictPath));
        jiebaSegmenter = new JiebaSegmenter();
    }
}
