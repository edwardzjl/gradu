package document_clustering.init;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by edwardlol on 2016/12/3.
 */
public class Init8703Mapper extends Mapper<LongWritable, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private Map<Pattern, String> synonymsMap = new HashMap<>();

    private StringBuilder stringBuilder = new StringBuilder();

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
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
        String[] nameAndModel = replaceSynonyms(contents[5]).split("@@");

        this.stringBuilder.setLength(0);
        this.stringBuilder = SepUtils.appendByAnsj(this.stringBuilder, nameAndModel[0]).append("##");
        // TODO: 17-2-22 make this a choice
//        this.stringBuilder = SepUtils.appendByJieba(this.stringBuilder, nameAndModel[0]).append("##");
        if (nameAndModel.length == 2) {
            this.stringBuilder = SepUtils.appendByAnsj(this.stringBuilder, nameAndModel[1]);
//            this.stringBuilder = SepUtils.appendByJieba(this.stringBuilder, nameAndModel[1]);
        }
        this.outputKey.set(contents[0] + "@@" + contents[1]);
        this.outputValue.set(this.stringBuilder.toString());
        context.write(this.outputKey, this.outputValue);
    }


    /**
     * @param origin
     * @return
     */
    private String replaceSynonyms(String origin) {
        String result = origin;
        for (Map.Entry<Pattern, String> entry : this.synonymsMap.entrySet()) {
            result = entry.getKey().matcher(result).replaceAll(entry.getValue());
        }
        return result;
    }
}

// End Init8703Mapper.java
