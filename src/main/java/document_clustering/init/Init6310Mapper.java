package document_clustering.init;

import com.huaban.analysis.jieba.JiebaSegmenter;
import document_clustering.util.JiebaFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by edwardlol on 2016/12/3.
 */
// TODO: 2016/12/25 not finished, 6310 seems to be messy
public class Init6310Mapper extends Mapper<LongWritable, Text, Text, Text> {
    //~ Instance fields --------------------------------------------------------

    private Text outputKey = new Text();

    private Text outputValue = new Text();

    private JiebaFactory jieba;

    //~ Methods ----------------------------------------------------------------

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String dictpath = conf.get("dict.path");
        jieba = JiebaFactory.getInstance(dictpath);
    }

    /**
     *
     * @param key     position
     * @param value   entry_id,d_date,i_e_port,decl_port,contr_no,trade_mode,trade_co,trade_name,
     *                owner_code,owner_name,agent_name,trade_country,code_ts,,g_name,[g_model],decl_price,
     *                decl_total,g_qty,g_unit,qty_1,unit_1,usd_price,real_rsv1,pack_no
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] contents = value.toString().replaceAll("阿拉比加", "阿拉比卡")
                .replaceAll("罗[布巴伯]斯[特塔]", "罗布斯塔")
                .replaceAll("[培烘]炒", "焙炒")
                .split("@@");

        this.outputKey.set(contents[0] + "@@" + contents[1]);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder = append(stringBuilder, contents[4]);
        stringBuilder.append("##");

        if (contents.length == 6) {
            stringBuilder = append(stringBuilder, contents[5]);
        }
        this.outputValue.set(stringBuilder.toString());
        context.write(this.outputKey, this.outputValue);

    }

    /**
     * append the seperate result to the StringBuilder
     *
     * @param stringBuilder stringBuilder to append to
     * @param sentence      sentence to be seperated
     * @return stringBuilder
     */
    private StringBuilder append(StringBuilder stringBuilder, String sentence) {
        List<String> terms = jieba.seperate(sentence, JiebaSegmenter.SegMode.SEARCH);
        Iterator<String> iterator = terms.iterator();
        while (iterator.hasNext()) {
            stringBuilder.append(iterator.next());
            if (iterator.hasNext()) {
                stringBuilder.append(" ");
            }
        }
        return stringBuilder;
    }
}

// End Init6310Mapper.java
