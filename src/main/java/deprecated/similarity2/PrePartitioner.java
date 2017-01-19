package deprecated.similarity2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by edwardlol on 2016/12/27.
 */
public class PrePartitioner extends Partitioner<Text, Text> {
    //~ Static fields/initializers ---------------------------------------------

    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    //~ Methods ----------------------------------------------------------------

    @Override
    public int getPartition(Text term, Text indice, int numPartitions) {

//        String[] docs = indice.toString().split(",");
//
//        if (docs.length > 1000) {
//            int contentsInBlock = docs.length / 6;
//            if (docs.length % 6 != 0) {
//                contentsInBlock++;
//            }
//
//            for (int i = 0; i < 5; i++) {
//                StringBuilder output = new StringBuilder();
//                for (int a = 0; a < contentsInBlock; a++) {
//                    output.append(docs[i * contentsInBlock + a]).append(',');
//                }
//                for (int j = i + 1; j < 6; j++) {
//                    for (int b = 0; b < contentsInBlock; b++) {
//                        int index = j * contentsInBlock + b;
//                        if (index >= docs.length) {
//                            break;
//                        }
//                        output.append(docs[index]).append(',');
//                    }
//                    this.outputKey.set(bigIndex++);
//                    this.outputValue.set(key.toString() + ":"
//                            + output.deleteCharAt(output.length() - 1).toString());
//                    // container \t term:doc_line_no=tf-idf,...
//                    context.write(this.outputKey, this.outputValue);
//                    bigIndex = bigIndex % this.reduceNum;
//                }
//            }
//        } else if (docs.length > 1) {
//            this.outputKey.set(smallIndex++);
//            this.outputValue.set(key.toString() + ":" + value.toString());
//            // container \t term:doc_line_no=tf-idf,...
//            context.write(this.outputKey, this.outputValue);
//            smallIndex = smallIndex % this.reduceNum;
//        }
        return 0;
    }
}

// End PrePartitioner.java
