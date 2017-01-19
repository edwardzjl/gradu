package document_clustering.util;

import com.google.common.collect.Lists;
import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;
import com.huaban.analysis.jieba.WordDictionary;

import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by edwardlol on 2016/12/16.
 */
public class JiebaFactory {
    //~ Static fields/initializers ---------------------------------------------

    private static JiebaFactory instance;
    public JiebaSegmenter segmenter = new JiebaSegmenter();

    //~ Instance fields --------------------------------------------------------

    private JiebaFactory(String dictPath) {
        WordDictionary wordDictionary = WordDictionary.getInstance();
        wordDictionary.init(Paths.get(dictPath));
    }

    //~ Constructors -----------------------------------------------------------

    public static JiebaFactory getInstance(String dictPath) {
        if (instance == null) {
            instance = new JiebaFactory(dictPath);
        }
        return instance;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * seperate words in sentence
     *
     * @param sentence the sentence you want to seperate
     * @return the list of terms in the sentence
     */
    public List<String> seperate(String sentence, JiebaSegmenter.SegMode mode) {
        List<String> termList = Lists.newArrayList();
        // seperate tokens
        List<SegToken> tokens = segmenter.process(sentence, mode);
        // delete punctuation
        String regex = "^[\\pP+~$`^=|<>～｀＄＾＋＝｜＜＞￥×\\s]+$|" +
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
        Pattern pattern = Pattern.compile(regex);
        tokens.forEach(token -> {
            Matcher matcher = pattern.matcher(token.word);
            if (!matcher.find()) {
                termList.add(token.word);
            }
        });
        return termList;
    }
}

// End JiebaFactory.java
