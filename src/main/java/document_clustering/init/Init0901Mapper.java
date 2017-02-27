package document_clustering.init;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * init 0901 comodity with ansj
 * Created by edwardlol on 2016/12/3.
 */
public class Init0901Mapper extends Mapper<LongWritable, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private Map<Pattern, String> synonymsMap = new HashMap<>();

    private StringBuilder stringBuilder = new StringBuilder();

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.synonymsMap.put(Pattern.compile("阿拉[比毕][加卡]"), "阿拉比卡");
        this.synonymsMap.put(Pattern.compile("罗[布巴伯]斯[特塔]"), "罗布斯塔");
        this.synonymsMap.put(Pattern.compile("[培烘]炒"), "焙炒");
        this.synonymsMap.put(Pattern.compile("烘培"), "焙炒");
        this.synonymsMap.put(Pattern.compile("寝除"), "浸除");
    }

    /**
     * @param key     position
     * @param value   entry_id@@g_no@@code_ts@@decl_port@@g_name@@[g_model]
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] contents = replaceSynonyms(value.toString()).split("@@");

        this.stringBuilder.setLength(0);
        this.stringBuilder = SepUtils.appendByAnsj(this.stringBuilder, contents[4]).append("##");
        // TODO: 17-2-22 make this a choice
//        this.stringBuilder = SepUtils.appendByJieba(this.stringBuilder, contents[4]).append("##");
        if (contents.length == 6) {
            this.stringBuilder = SepUtils.appendByAnsj(this.stringBuilder, contents[5]);
            //        this.stringBuilder = SepUtils.appendByJieba(this.stringBuilder, contents[5]);
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

// End Init0901Mapper.java
