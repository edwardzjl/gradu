package document_clustering.init;

import com.huaban.analysis.jieba.JiebaSegmenter;
import document_clustering.util.JiebaFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class Init8703Mapper extends Mapper<LongWritable, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private JiebaFactory jieba;

    private Map<Pattern, String> synonymsMap = new HashMap<>();

    private StringBuilder stringBuilder = new StringBuilder();

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String dictpath = conf.get("dict.path");
        jieba = JiebaFactory.getInstance(dictpath);

        synonymsMap.put(Pattern.compile("5座"), "五座");
        synonymsMap.put(Pattern.compile("7座"), "七座");
        synonymsMap.put(Pattern.compile("不是"), "非");
        synonymsMap.put(Pattern.compile("4maitc"), "4matic");
        synonymsMap.put(Pattern.compile("4mat[12]c"), "4matic");
        synonymsMap.put(Pattern.compile("(?i)can-am"), "canam");
        synonymsMap.put(Pattern.compile("cfm0to"), "cfmoto");
        synonymsMap.put(Pattern.compile("bmw"), "宝马");
        synonymsMap.put(Pattern.compile("benz"), "奔驰");
        synonymsMap.put(Pattern.compile("audi"), "奥迪");
        synonymsMap.put(Pattern.compile("mercecles"), "mercedes");
        synonymsMap.put(Pattern.compile("mercede"), "mercedes");
        synonymsMap.put(Pattern.compile("ferraei"), "ferrari");
        synonymsMap.put(Pattern.compile("ferrair"), "ferrari");
        synonymsMap.put(Pattern.compile("一气"), "一汽");
        synonymsMap.put(Pattern.compile("三凌"), "三菱");
        synonymsMap.put(Pattern.compile("克[来菜]斯勒"), "克莱斯勒");
        synonymsMap.put(Pattern.compile("二厢"), "两厢");
        synonymsMap.put(Pattern.compile("保费"), "保险费");
        synonymsMap.put(Pattern.compile("爱玛仕"), "爱马仕");
        synonymsMap.put(Pattern.compile("乌拉斯"), "乌阿斯");
        synonymsMap.put(Pattern.compile("全地[行型]"), "全地形");
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

        String[] nameAndModel = replaceSyno(contents[5]).split("@@");

        stringBuilder.setLength(0);

        stringBuilder = append(stringBuilder, nameAndModel[0]).append("##");

        if (nameAndModel.length == 2) {
            stringBuilder = append(stringBuilder, nameAndModel[1]);
        }

        this.outputKey.set(contents[0] + "@@" + contents[1]);
        this.outputValue.set(stringBuilder.toString());
        context.write(this.outputKey, this.outputValue);
    }


    private String replaceSyno(String term) {
        String result = term;
        for (Map.Entry<Pattern, String> entry : this.synonymsMap.entrySet()) {
            result = entry.getKey().matcher(result).replaceAll(entry.getValue());
        }
        return result;
    }

    /**
     * append the seperate result to the StringBuilder
     *
     * @param sentence      sentence to be seperated
     * @return stringBuilder
     */
    private StringBuilder append(StringBuilder stringBuilder, String sentence) {
        List<String> terms = jieba.seperate(sentence, JiebaSegmenter.SegMode.SEARCH);
        if (terms.size() > 0) {
            for (String term : terms) {
                stringBuilder.append(term).append(' ');
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        }
        return stringBuilder;
    }
}

// End Init0901Mapper.java
