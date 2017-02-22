package word_sep;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.junit.Test;

/**
 * Created by edwardlol on 17-2-21.
 */
public class SepTest {

    @Test
    public void sep() {
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
}
