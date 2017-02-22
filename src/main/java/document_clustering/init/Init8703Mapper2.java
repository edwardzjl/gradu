package document_clustering.init;

import com.huaban.analysis.jieba.JiebaSegmenter;
import document_clustering.util.JiebaFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.synonym.SynonymFilterFactory;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.FilesystemResourceLoader;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class Init8703Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private JiebaFactory jieba;

    private StringBuilder stringBuilder = new StringBuilder();

    private SynonymFilterFactory synonymFilterFactory;

    private WhitespaceAnalyzer whitespaceAnalyzer;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String dictpath = conf.get("dict.path");
        jieba = JiebaFactory.getInstance(dictpath);
        Version ver = Version.LUCENE_46;
        Map<String, String> filterArgs = new HashMap<>();
        filterArgs.put("luceneMatchVersion", ver.toString());
        filterArgs.put("synonyms", "./dicts/synonymsLuc.dic");
        filterArgs.put("expand", "true");
        synonymFilterFactory = new SynonymFilterFactory(filterArgs);
        synonymFilterFactory.inform(new FilesystemResourceLoader());
        whitespaceAnalyzer = new WhitespaceAnalyzer(ver);
    }

    /**
     *
     * @param key     position
     * @param value   entry_id \t g_no \t code_ts \t g_name \t country \t g_name@@[g_model]
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] contents = value.toString().split("\t");
        String[] nameAndModel = contents[5].split("@@");

        TokenStream ts_name = synonymFilterFactory.create(whitespaceAnalyzer
                .tokenStream("someField", append(nameAndModel[0])));

        String g_name = convert(ts_name);
        stringBuilder.append("##");

        String g_model = "";
        if (nameAndModel.length == 2) {
            TokenStream ts_model = synonymFilterFactory.create(whitespaceAnalyzer
                    .tokenStream("someField", append(nameAndModel[1])));
            g_model = convert(ts_model);
        }

        this.outputKey.set(contents[0] + "@@" + contents[1]);
        this.outputValue.set(g_name + "##" + g_model);
        context.write(this.outputKey, this.outputValue);
    }

    /**
     * append the seperate result to the StringBuilder
     *
     * @param sentence      sentence to be seperated
     * @return stringBuilder
     */
    private String append(String sentence) {
        stringBuilder.setLength(0);
        List<String> terms = jieba.seperate(sentence, JiebaSegmenter.SegMode.SEARCH);
        if (terms.size() > 0) {
            for (String term : terms) {
                stringBuilder.append(term).append(' ');
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            return stringBuilder.toString();
        } else {
            return "";
        }
    }

    private String convert(TokenStream ts) throws IOException {
        stringBuilder.setLength(0);
        CharTermAttribute termAttr = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
            stringBuilder.append(termAttr).append(' ');
        }
        try {
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        } catch (Exception e) {
            System.err.println(ts.toString());
        }

        ts.end();
        ts.close();
        return stringBuilder.toString();
    }
}

// End Init8703Mapper2.java
