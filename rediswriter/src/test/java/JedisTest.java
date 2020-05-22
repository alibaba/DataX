import com.alibaba.datax.plugin.writer.rediswriter.RedisWriterHelper;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.PipelineBase;

/**
 * @author lijf@2345.com
 * @date 2020/5/21 19:13
 * @desc
 */
public class JedisTest {
    public static void main(String[] args) {
        JedisCluster je = RedisWriterHelper.getJedisCluster("recessw-web-redis002:6379,recessw-web-redis002:6479,recessw-web-redis003:6379,recessw-web-redis003:6479,recessw-web-redis004:6379,recessw-web-redis004:6479", "Pye9WQAYsgetVrLw");
//        PipelineBase pipeLine = RedisWriterHelper.getPipeLine(je);
//        pipeLine.set("111","111");
//        pipeLine.set("222","222");
//        pipeLine.set("333","333");
//        RedisWriterHelper.syscData(pipeLine);
        String s1 = je.get("datax:333");

        System.out.println(s1);
    }
}
