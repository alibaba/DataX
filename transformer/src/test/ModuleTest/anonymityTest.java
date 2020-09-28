package ModuleTest;
import com.alibaba.datax.transformer.maskingMethods.anonymity.EnumerateMasker;
import com.alibaba.datax.transformer.maskingMethods.anonymity.FloorMasker;
import com.alibaba.datax.transformer.maskingMethods.anonymity.Hiding;
import com.alibaba.datax.transformer.maskingMethods.anonymity.PrefixPreserveMasker;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by LabUser on 2018/5/8.
 */
public class anonymityTest {

    @Test
    public void hidingTest(){
        Hiding masker = new Hiding();
        System.out.println("Hiding全掩盖脱敏:");
        try{
            System.out.println("123 -> " + masker.mask(123));
            System.out.println("1.023 -> " + masker.mask(1.023));
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy年MM月dd日");
            String strDate = "2005年04月22日";
            Date date = simpleDateFormat.parse(strDate);
            System.out.println("2005年04月22日->" + masker.mask(date));
            String origin_str = "什么鬼";
            System.out.println(origin_str + "-> " + masker.mask(origin_str));
            boolean origin_bool = false;
            System.out.println(origin_bool + "-> " + masker.mask(origin_bool));
//            String strDate="2005年04月22日";
//            //注意：SimpleDateFormat构造函数的样式与strDate的样式必须相符
//
//            SimpleDateFormat sDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //加上时间
//            //必须捕获异常
        } catch(Exception px) {
            px.printStackTrace();
        }
    }

    @Test
    public void enumerateMaskerTest(){
        System.out.println("保序脱敏:");
        try {
            for(int i=10;i<15;i++) {
                System.out.println("Enumerate Masker "+ i +" -> " + EnumerateMasker.mask(i, 100));
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void prefixPreserveTest(){
        System.out.println("前缀保留保型脱敏:");
        try {
            String origin = "192.168.1.101";
            System.out.println("Prefix preserving "+origin+" -> "+PrefixPreserveMasker.mask(origin, 7));
            long origin_long = 18788888888L;
            System.out.println("Prefix preserving "+origin_long+" -> "+PrefixPreserveMasker.mask(origin_long, 3));
            String bank_card_num = "6217002710000684874";
            System.out.println("Prefix preserving "+ bank_card_num +" -> "+PrefixPreserveMasker.mask(bank_card_num, 6));
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void floorTest(){
        System.out.println("取下界脱敏tests:");
        try{
            int origin_int = 1688;
            System.out.println("int "+ origin_int + " " + FloorMasker.mask((long) origin_int, 10));
            Double origin_double = -12.68;
            System.out.println( "Double " + origin_double + " " + FloorMasker.mask(origin_double));
            Date ori_date = new Date();
            System.out.println("对于今天是几号的取下界脱敏 "+ori_date+"->"+FloorMasker.mask(ori_date, "YMDHms"));
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

}
