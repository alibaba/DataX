package com.alibaba.datax.plugin.reader.obsreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hdfsreader.HdfsReader;
import com.alibaba.datax.plugin.reader.obsreader.util.HdfsParquetUtil;
import com.alibaba.datax.plugin.reader.obsreader.util.ObsSplitUtil;
import com.alibaba.datax.plugin.reader.obsreader.util.ObsUtil;
import com.alibaba.datax.plugin.unstructuredstorage.FileFormat;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.datax.plugin.unstructuredstorage.reader.binaryFileUtil.BinaryFileReaderUtil;
import com.alibaba.datax.plugin.unstructuredstorage.reader.split.StartEndPair;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ObsReader extends Reader {

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(ObsReader.Job.class);
        private Configuration readerOriginConfig = null;
        private ObsClient obsClient = null;

        private String endpoint;
        private String accessKey;
        private String secretKey;
        private String bucket;

        private boolean successOnNoObject;
        private Boolean isBinaryFile;

        private String fileFormat;

        private HdfsReader.Job hdfsReaderJob;
        private boolean useHdfsReaderProxy = false;

        private List<String> objects;
        private List<Pair<String, Long>> objectSizePairs; /*用于任务切分的依据*/

        @Override
        public void init() {
            LOG.debug("init() begin...");
            this.readerOriginConfig = this.getPluginJobConf();
            this.basicValidateParameter();
            this.fileFormat = this.readerOriginConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FILE_FORMAT,
                    com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_FILE_FORMAT);
            this.useHdfsReaderProxy  = HdfsParquetUtil.isUseHdfsWriterProxy(this.fileFormat);
            if(useHdfsReaderProxy){
                HdfsParquetUtil.adaptConfiguration(this.readerOriginConfig);
                this.hdfsReaderJob = new HdfsReader.Job();
                this.hdfsReaderJob.setJobPluginCollector(this.getJobPluginCollector());
                this.hdfsReaderJob.setPeerPluginJobConf(this.getPeerPluginJobConf());
                this.hdfsReaderJob.setPeerPluginName(this.getPeerPluginName());
                this.hdfsReaderJob.setPluginJobConf(this.getPluginJobConf());
                this.hdfsReaderJob.init();
                return;
            }

            this.isBinaryFile = FileFormat.getFileFormatByConfiguration(this.readerOriginConfig).isBinary();
            this.validate();
            UnstructuredStorageReaderUtil.validateCsvReaderConfig(this.readerOriginConfig);
            this.successOnNoObject = this.readerOriginConfig.getBool(
                    Key.SUCCESS_ON_NO_Object, false);
            LOG.debug("init() ok and end...");
        }

        private void basicValidateParameter(){
            endpoint = this.readerOriginConfig.getString(Key.ENDPOINT);
            if (StringUtils.isBlank(endpoint)) {
                throw DataXException.asDataXException(
                        ObsReaderErrorCode.CONFIG_INVALID_EXCEPTION,"invalid endpoint");
            }

            accessKey = this.readerOriginConfig.getString(Key.ACCESSKEY);
            if (StringUtils.isBlank(accessKey)) {
                throw DataXException.asDataXException(
                        ObsReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "invalid accessKey");
            }

            secretKey = this.readerOriginConfig.getString(Key.SECRETKEY);
            if (StringUtils.isBlank(secretKey)) {
                throw DataXException.asDataXException(
                        ObsReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "invalid accessId");
            }
        }

        // warn: 提前验证endpoint,accessId,accessKey,bucket,object的有效性
        private void validate() {
            // fxxk
            // obsClient = new ObsClient(endpoint,accessId,accessKey);
            obsClient = ObsUtil.initObsClient(this.readerOriginConfig);


            bucket = this.readerOriginConfig.getString(Key.BUCKET);
            if (StringUtils.isBlank(bucket)) {
                throw DataXException.asDataXException(
                        ObsReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "invalid bucket");
            }else if(!obsClient.headBucket(bucket)){
                // 桶不存在
                throw DataXException.asDataXException(
                        ObsReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "invalid bucket");
            }

            String object = this.readerOriginConfig.getString(Key.OBJECT);
            if (StringUtils.isBlank(object)) {
                throw DataXException.asDataXException(
                        ObsReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "invalid object");
            }

            if (this.isBinaryFile){
                return;
            }
            UnstructuredStorageReaderUtil.validateParameter(this.readerOriginConfig);
        }

        @Override
        public void prepare() {
            if(useHdfsReaderProxy){
                this.hdfsReaderJob.prepare();
                return;
            }
            // 将每个单独的 object 作为一个 slice
            this.objectSizePairs = parseOriginObjectSizePairs(readerOriginConfig.getList(Key.OBJECT, String.class));
            this.objects = parseOriginObjects(readerOriginConfig.getList(Key.OBJECT, String.class));
            UnstructuredStorageReaderUtil.setSourceFileName(readerOriginConfig, this.objects);
            UnstructuredStorageReaderUtil.setSourceFile(readerOriginConfig, this.objects);
        }

        private List<Pair<String, Long>> parseOriginObjectSizePairs(List<String> originObjects) {
            List<Pair<String, Long>> parsedObjectSizePaires = new ArrayList<Pair<String, Long>>();

            for (String object : originObjects) {
                int firstMetaChar = (object.indexOf('*') > object.indexOf('?')) ? object
                        .indexOf('*') : object.indexOf('?');

                if (firstMetaChar != -1) {
                    int lastDirSeparator = object.lastIndexOf(
                            IOUtils.DIR_SEPARATOR, firstMetaChar);
                    String parentDir = object
                            .substring(0, lastDirSeparator + 1);
                    List<Pair<String, Long>> allRemoteObjectSizePairs = getAllRemoteObjectsKeyAndSizeInDir(parentDir);
                    Pattern pattern = Pattern.compile(object.replace("*", ".*")
                            .replace("?", ".?"));

                    for (Pair<String, Long> remoteObjectSizePair : allRemoteObjectSizePairs) {
                        if (pattern.matcher(remoteObjectSizePair.getKey()).matches()) {
                            parsedObjectSizePaires.add(remoteObjectSizePair);
                            LOG.info(String
                                    .format("add object [%s] as a candidate to be read.",
                                            remoteObjectSizePair.getKey()));
                        }
                    }
                } else {
                    // 如果没有配正则匹配,那么需要对用户自己配置的object存在性进行检测
                    try{
                        obsClient.getObject(bucket, object);
                        ObjectMetadata objMeta = obsClient.getObjectMetadata(bucket, object);
                        parsedObjectSizePaires.add(new MutablePair<String, Long>(object, objMeta.getContentLength() <= ObsSplitUtil.SINGLE_FILE_SPLIT_THRESHOLD_IN_SIZE ? -1L : objMeta.getContentLength()));
                        LOG.info(String.format(
                                "add object [%s] as a candidate to be read.",
                                object));
                    }catch (Exception e){
                        trackObsDetailException(e, object);
                    }
                }
            }
            return parsedObjectSizePaires;
        }

        private List<Pair<String, Long>> getAllRemoteObjectsKeyAndSizeInDir(String parentDir)
                throws ObsException {
            List<Pair<String, Long>> objectSizePairs = new ArrayList<Pair<String, Long>>();
            List<ObjectListing> objectListings  = getRemoteObjectListings(parentDir);

            if (objectListings.size() == 0) {
                return objectSizePairs;
            }

            for (ObjectListing objectList : objectListings){
//                for (ObsObject objectSummary : objectList.getObjects()) {
//                    Pair<String, Long> objNameSize = new MutablePair<String, Long>(objectSummary.getObjectKey(), objectSummary.getMetadata().getContentLength() <= ObsSplitUtil.SINGLE_FILE_SPLIT_THRESHOLD_IN_SIZE ? -1L : objectSummary.getSize());
////                    Pair<String, Long> objNameSize = new MutablePair<String, Long>(objectSummary.getObjectKey(), -1L);
//                    objectSizePairs.add(objNameSize);
//                }
                for (S3Object objectSummary : objectList.getObjectSummaries()) {
                    // TODO 没有objectSummary.getSize()这个方法，临时先用 -1L代替
                    Pair<String, Long> objNameSize = new MutablePair<String, Long>(objectSummary.getObjectKey(), objectSummary.getMetadata().getContentLength() <= ObsSplitUtil.SINGLE_FILE_SPLIT_THRESHOLD_IN_SIZE ? -1L : objectSummary.getMetadata().getContentLength());
//                    Pair<String, Long> objNameSize = new MutablePair<String, Long>(objectSummary.getObjectKey(), -1L);
                    objectSizePairs.add(objNameSize);
                }
            }

            return  objectSizePairs;
        }

        private List<ObjectListing> getRemoteObjectListings(String parentDir) throws ObsException {

            List<ObjectListing> remoteObjectListings = new ArrayList<ObjectListing>();

            LOG.debug("Parent folder: {}", parentDir);
            List<String> remoteObjects = new ArrayList<String>();
            ObsClient client = ObsUtil.initObsClient(readerOriginConfig);

            try {
                ListObjectsRequest listObjectsRequest = new ListObjectsRequest(
                        readerOriginConfig.getString(Key.BUCKET));
                listObjectsRequest.setPrefix(parentDir);
                ObjectListing remoteObjectList;
                do {
                    remoteObjectList = client.listObjects(listObjectsRequest);
                    if (null != remoteObjectList) {
                        LOG.info("ListObjects prefix: {} requestId: {}", remoteObjectList.getPrefix(), remoteObjectList.getRequestId());
                    } else {
                        LOG.info("ListObjectsRequest get null");
                    }
                    remoteObjectListings.add(remoteObjectList);
                    listObjectsRequest.setMarker(remoteObjectList.getNextMarker());
                    LOG.debug(listObjectsRequest.getMarker());
                    LOG.debug(String.valueOf(remoteObjectList.isTruncated()));
                } while (remoteObjectList.isTruncated());
            } catch (ObsException e) {
                trackObsDetailException(e, null);
            }

            return remoteObjectListings;
        }

        // 对obs配置异常信息进行细分定位
        private void trackObsDetailException(Exception e, String object){
            // 对异常信息进行细分定位
            String errorMessage = e.getMessage();
            if(StringUtils.isNotBlank(errorMessage)){
                if(errorMessage.contains("UnknownHost")){
                    // endPoint配置错误
                    throw DataXException.asDataXException(
                            ObsReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "The endpoint you configured is not correct. Please check the endpoint configuration", e);
                }else if(errorMessage.contains("InvalidAccessKeyId")){
                    // accessId配置错误
                    throw DataXException.asDataXException(
                            ObsReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "The accessId you configured is not correct. Please check the accessId configuration", e);
                }else if(errorMessage.contains("SignatureDoesNotMatch")){
                    // accessKey配置错误
                    throw DataXException.asDataXException(
                            ObsReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "The accessKey you configured is not correct. Please check the accessId configuration", e);
                }else if(errorMessage.contains("NoSuchKey")){
                    if (e instanceof ObsException) {
                        ObsException obsException = (ObsException) e;
                        if ("NoSuchKey".equalsIgnoreCase(obsException
                                .getErrorCode()) && this.successOnNoObject) {
                            LOG.warn(String.format("obs file %s is not exits to read:", object), e);
                            return;
                        }
                    }
                    // object配置错误
                    throw DataXException.asDataXException(
                            ObsReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "The object you configured is not correct. Please check the accessId configuration");
                }else{
                    // 其他错误
                    throw DataXException.asDataXException(
                            ObsReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            String.format("Please check whether the configuration of [endpoint], [accessId], [accessKey], [bucket], and [object] are correct. Error reason: %s",e.getMessage()), e);
                }
            }else{
                throw DataXException.asDataXException(
                        ObsReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "The configured json is invalid", e);
            }
        }

        private List<String> parseOriginObjects(List<String> originObjects) {
            List<String> objList = new ArrayList<>();

            if (this.objectSizePairs == null) {
                this.objectSizePairs = parseOriginObjectSizePairs(originObjects);
            }

            for (Pair<String, Long> objSizePair : this.objectSizePairs) {
                objList.add(objSizePair.getKey());
            }

            return objList;
        }

        @Override
        public void post() {
            if(useHdfsReaderProxy){
                this.hdfsReaderJob.post();
                return;
            }
            LOG.debug("post()");
        }

        @Override
        public void destroy() {
            if(useHdfsReaderProxy){
                this.hdfsReaderJob.destroy();
                return;
            }
            LOG.debug("destroy()");
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.debug("split() begin...");
            if(useHdfsReaderProxy){
                return  hdfsReaderJob.split(adviceNumber);
            }
            List<Configuration> readerSplitConfigs;

            if (0 == objects.size() && this.successOnNoObject) {
                readerSplitConfigs = new ArrayList<Configuration>();
                Configuration splitedConfig = this.readerOriginConfig.clone();
                splitedConfig.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.SPLIT_SLICE_CONFIG, null);
                readerSplitConfigs.add(splitedConfig);
                LOG.info(String.format("no Obs object to be read"));
                LOG.debug("split() ok and end...");
                return readerSplitConfigs;
            }else if (0 == objects.size()) {
                throw DataXException.asDataXException(
                        ObsReaderErrorCode.EMPTY_BUCKET_EXCEPTION,
                        String.format("Unable to find the object to read. Please confirm your configured item [bucket]: %s object: %s",
                                this.readerOriginConfig.get(Key.BUCKET),
                                this.readerOriginConfig.get(Key.OBJECT)));
            }

            /**
             * 当文件类型是text纯文本文件,并且不是压缩的情况下,
             * 可以对纯文本文件进行内部切分实现并发读取, 如果用户不希望对文件拆分, 可以指定fileFormat为csv
             *
             * 注意：这里判断文件是否为text以及是否压缩，信息都是通过任务配置项来获取的
             *
             * 这里抽出一个方法来判断是否需要分片
             * */
            ObsSplitUtil obsFileSplit = new ObsSplitUtil(this.obsClient, this.bucket);
            long t1 = System.currentTimeMillis();
            readerSplitConfigs = obsFileSplit.getSplitedConfigurations(this.readerOriginConfig, this.objectSizePairs,
                    adviceNumber);
            long t2 = System.currentTimeMillis();
            LOG.info("all split done, cost {}ms", t2 - t1);
            /**
             * 在日志中告知用户,为什么实际datax切分跑的channel数会小于用户配置的channel数
             * 注意：这里的报告的原因不准确，报的原因是一个文件一个task，所以最终切分数小于用户配置数，实际原因还有单文件切分时，
             * 单文件的大小太小（理论64M一个block），导致问题比较少
             */
            if(readerSplitConfigs.size() < adviceNumber){
                LOG.info("[Note]: During ObsReader data synchronization, one file can only be synchronized in one task. You want to synchronize {} files " +
                                "and the number is less than the number of channels you configured: {}. " +
                                "Therefore, please take note that DataX will actually have {} sub-tasks, that is, the actual concurrent channels = {}",
                        objects.size(), adviceNumber, objects.size(), objects.size());
            }
            LOG.info("split() ok and end...");
            return readerSplitConfigs;
        }
    }

    public static class Task extends Reader.Task {

        private static final Logger LOG = LoggerFactory.getLogger(ObsReader.Job.class);

        private Configuration readerSliceConfig;
        private Boolean isBinaryFile;
        private Integer blockSizeInByte;
        private List<StartEndPair> allWorksForTask;
        private boolean originSkipHeader;
        private ObsClient obsClient;
        private String fileFormat;
        private HdfsReader.Task hdfsReaderTask;
        private boolean useHdfsReaderProxy = false;

        @Override
        public void init() {
            this.readerSliceConfig = this.getPluginJobConf();
            this.fileFormat = this.readerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FILE_FORMAT,
                    com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_FILE_FORMAT);
            this.useHdfsReaderProxy  = HdfsParquetUtil.isUseHdfsWriterProxy(this.fileFormat);
            if(useHdfsReaderProxy){
                this.hdfsReaderTask = new HdfsReader.Task();
                this.hdfsReaderTask.setPeerPluginJobConf(this.getPeerPluginJobConf());
                this.hdfsReaderTask.setPeerPluginName(this.getPeerPluginName());
                this.hdfsReaderTask.setPluginJobConf(this.getPluginJobConf());
                this.hdfsReaderTask.setReaderPluginSplitConf(this.getReaderPluginSplitConf());
                this.hdfsReaderTask.setTaskGroupId(this.getTaskGroupId());
                this.hdfsReaderTask.setTaskId(this.getTaskId());
                this.hdfsReaderTask.setTaskPluginCollector(this.getTaskPluginCollector());
                this.hdfsReaderTask.init();
                return;
            }
            String allWorksForTaskStr = this.readerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.SPLIT_SLICE_CONFIG);
            if (StringUtils.isBlank(allWorksForTaskStr)) {
                allWorksForTaskStr = "[]";
            }
            this.allWorksForTask = JSON.parseObject(allWorksForTaskStr, new TypeReference<List<StartEndPair>>() {
            });
            this.isBinaryFile = FileFormat.getFileFormatByConfiguration(this.readerSliceConfig).isBinary();
            this.blockSizeInByte = this.readerSliceConfig.getInt(
                    com.alibaba.datax.plugin.unstructuredstorage.reader.Key.BLOCK_SIZE_IN_BYTE,
                    com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_BLOCK_SIZE_IN_BYTE);
            this.originSkipHeader = this.readerSliceConfig
                    .getBool(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.SKIP_HEADER, false);
        }

        @Override
        public void prepare() {
            LOG.info("task prepare() begin...");
            if(useHdfsReaderProxy){
                this.hdfsReaderTask.prepare();
                return;
            }
        }

        @Override
        public void destroy() {
            if(useHdfsReaderProxy){
                this.hdfsReaderTask.destroy();
                return;
            }
            try {
                // this.obsClient.shutdown();
            } catch (Exception e) {
                LOG.warn("shutdown obsclient meet a exception:" + e.getMessage(), e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            if(useHdfsReaderProxy){
                this.hdfsReaderTask.startRead(recordSender);
                return;
            }
            boolean successOnNoObject = this.readerSliceConfig.getBool(Key.SUCCESS_ON_NO_Object, false);
            if (this.allWorksForTask.isEmpty() && successOnNoObject) {
                recordSender.flush();
                return;
            }
            String bucket = this.readerSliceConfig.getString(Key.BUCKET);
            this.obsClient = ObsUtil.initObsClient(this.readerSliceConfig);
            for (StartEndPair eachSlice : this.allWorksForTask) {
                String object = eachSlice.getFilePath();
                Long start = eachSlice.getStart();
                Long end = eachSlice.getEnd();
                LOG.info(String.format("read bucket=[%s] object=[%s], range: [start=%s, end=%s] start...", bucket,
                        object, start, end));
                InputStream obsInputStream = new ObsInputStream(obsClient, bucket, object, start, end);
                // 检查是否要跳过表头, 防止重复跳过首行
                Boolean skipHeaderValue = this.originSkipHeader && (0L == start);
                this.readerSliceConfig.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.SKIP_HEADER,
                        skipHeaderValue);
                try {
                    if (!this.isBinaryFile) {
                        UnstructuredStorageReaderUtil.readFromStream(obsInputStream, object, this.readerSliceConfig,
                                recordSender, this.getTaskPluginCollector());
                    } else {
                        BinaryFileReaderUtil.readFromStream(obsInputStream, object, recordSender, this.blockSizeInByte);
                    }
                } finally {
                    IOUtils.closeQuietly(obsInputStream);
                }
            }
            recordSender.flush();
        }

        @Override
        public void post() {
            LOG.info("task post() begin...");
            if(useHdfsReaderProxy){
                this.hdfsReaderTask.post();
                return;
            }
        }

    }

}
