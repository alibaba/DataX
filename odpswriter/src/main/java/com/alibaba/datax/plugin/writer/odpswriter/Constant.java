package com.alibaba.datax.plugin.writer.odpswriter;


public class Constant {
    public static final String SKYNET_ACCESSID = "SKYNET_ACCESSID";

    public static final String SKYNET_ACCESSKEY = "SKYNET_ACCESSKEY";

    public static final String DEFAULT_ACCOUNT_TYPE = "aliyun";

    public static final String TAOBAO_ACCOUNT_TYPE = "taobao";

    public static final String COLUMN_POSITION = "columnPosition";

    /*
     * 每个task独立维护一个proxy列表，一共会生成 task并发量 * 分区数量 的proxy，每个proxy会创建 blocksizeInMB（一般是64M） 大小的数组
     * 因此极易OOM，
     * 假设默认情况下768M的内存，实际最多只能创建 12 个proxy，8G内存最多只能创建126个proxy，所以最多只允许创建一定数量的proxy，对应到分区数量 1：1
     *
     * blockSizeInMB 减小可以减少内存消耗，但是意味着更高频率的网络请求，会对odps服务器造成较大压力
     *
     * 另外，可以考虑proxy不用常驻内存，但是需要增加复杂的控制逻辑
     * 但是一般情况下用户作为分区值得数据是有规律的，比如按照时间，2020-08的数据已经同步完成了，并且后面没有这个分区的数据了，对应的proxy还放在内存中，
     * 会造成很大的内存浪费。所以有必要对某些proxy进行回收。
     *
     * 这里采用是否回收某个proxy的标准是：在最近时间内是否有过数据传输。
     *
     *
     * 需要注意的问题！
     * 多个任务公用一个proxy，写入时需要抢锁，多并发的性能会受到很大影响，相当于单个分区时串行写入
     * 这个对性能影响很大，需要避免这种方式，还是尽量各个task有独立的proxy，只是需要去控制内存的使用，只能是控制每个task保有的proxy数量了
     *
     * 还可以考虑修改proxy的数组大小，但是设置太小不确定会不会影响性能。可以测试一下
     */

    public static final Long PROXY_MAX_IDLE_TIME_MS =60 * 1000L;  // 60s没有动作就回收

    public static final Long MAX_PARTITION_CNT = 200L;

    public static final int UTF8_ENCODED_CHAR_MAX_SIZE = 6;

    public static final int DEFAULT_FIELD_MAX_SIZE = 8 * 1024 * 1024;


}
