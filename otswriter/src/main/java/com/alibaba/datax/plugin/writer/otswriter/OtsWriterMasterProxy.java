package com.alibaba.datax.plugin.writer.otswriter;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.callable.GetTableMetaCallable;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf.RestrictConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.alibaba.datax.plugin.writer.otswriter.utils.ParamChecker;
import com.alibaba.datax.plugin.writer.otswriter.utils.RetryHelper;
import com.alibaba.datax.plugin.writer.otswriter.utils.WriterModelParser;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.TableMeta;

public class OtsWriterMasterProxy {
    
    private OTSConf conf = new OTSConf();
    
    private OTSClient ots = null;
    
    private TableMeta meta = null;
    
    private static final Logger LOG = LoggerFactory.getLogger(OtsWriterMasterProxy.class);
    
    /**
     * @param param
     * @throws Exception
     */
    public void init(Configuration param) throws Exception {
        
        // 默认参数
        conf.setRetry(param.getInt(OTSConst.RETRY, 18));
        conf.setSleepInMillisecond(param.getInt(OTSConst.SLEEP_IN_MILLISECOND, 100));
        conf.setBatchWriteCount(param.getInt(OTSConst.BATCH_WRITE_COUNT, 100));
        conf.setConcurrencyWrite(param.getInt(OTSConst.CONCURRENCY_WRITE, 5));
        conf.setIoThreadCount(param.getInt(OTSConst.IO_THREAD_COUNT, 1));
        conf.setSocketTimeout(param.getInt(OTSConst.SOCKET_TIMEOUT, 20000));
        conf.setConnectTimeout(param.getInt(OTSConst.CONNECT_TIMEOUT, 10000));
        conf.setBufferSize(param.getInt(OTSConst.BUFFER_SIZE, 1024));
        
        RestrictConf restrictConf = conf.new RestrictConf();
        restrictConf.setRequestTotalSizeLimition(param.getInt(OTSConst.REQUEST_TOTAL_SIZE_LIMITATION, 1024 * 1024));
        restrictConf.setAttributeColumnSize(param.getInt(OTSConst.ATTRIBUTE_COLUMN_SIZE_LIMITATION, 2 * 1024 * 1024));
        restrictConf.setPrimaryKeyColumnSize(param.getInt(OTSConst.PRIMARY_KEY_COLUMN_SIZE_LIMITATION, 1024));
        restrictConf.setMaxColumnsCount(param.getInt(OTSConst.ATTRIBUTE_COLUMN_MAX_COUNT, 1024));
        conf.setRestrictConf(restrictConf);

        // 必选参数
        conf.setEndpoint(ParamChecker.checkStringAndGet(param, Key.OTS_ENDPOINT)); 
        conf.setAccessId(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSID)); 
        conf.setAccessKey(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSKEY)); 
        conf.setInstanceName(ParamChecker.checkStringAndGet(param, Key.OTS_INSTANCE_NAME)); 
        conf.setTableName(ParamChecker.checkStringAndGet(param, Key.TABLE_NAME)); 
        
        conf.setOperation(WriterModelParser.parseOTSOpType(ParamChecker.checkStringAndGet(param, Key.WRITE_MODE)));
        
        ots = new OTSClient(
                this.conf.getEndpoint(),
                this.conf.getAccessId(),
                this.conf.getAccessKey(),
                this.conf.getInstanceName());
        
        meta = getTableMeta(ots, conf.getTableName());
        LOG.info("Table Meta : {}", GsonParser.metaToJson(meta));
        
        conf.setPrimaryKeyColumn(WriterModelParser.parseOTSPKColumnList(ParamChecker.checkListAndGet(param, Key.PRIMARY_KEY, true)));
        ParamChecker.checkPrimaryKey(meta, conf.getPrimaryKeyColumn());
        
        conf.setAttributeColumn(WriterModelParser.parseOTSAttrColumnList(ParamChecker.checkListAndGet(param, Key.COLUMN, conf.getOperation() == OTSOpType.UPDATE_ROW ? true : false)));
        ParamChecker.checkAttribute(conf.getAttributeColumn());
    }
    
    public List<Configuration> split(int mandatoryNumber){
        LOG.info("Begin split and MandatoryNumber : {}", mandatoryNumber);
        List<Configuration> configurations = new ArrayList<Configuration>();
        for (int i = 0; i < mandatoryNumber; i++) {
            Configuration configuration = Configuration.newDefault();
            configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(this.conf));
            configurations.add(configuration);
        }
        LOG.info("End split.");
        assert(mandatoryNumber == configurations.size());
        return configurations;
    }
    
    public void close() {
        ots.shutdown();
    }
    
    public OTSConf getOTSConf() {
        return conf;
    }

    // private function

    private TableMeta getTableMeta(OTSClient ots, String tableName) throws Exception {
        return RetryHelper.executeWithRetry(
                new GetTableMetaCallable(ots, tableName),
                conf.getRetry(),
                conf.getSleepInMillisecond()
                );
    }
}
