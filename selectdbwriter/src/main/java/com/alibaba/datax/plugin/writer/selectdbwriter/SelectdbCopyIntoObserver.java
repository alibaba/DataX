package com.alibaba.datax.plugin.writer.selectdbwriter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class SelectdbCopyIntoObserver {
    private static final Logger LOG = LoggerFactory.getLogger(SelectdbCopyIntoObserver.class);

    private Keys options;
    private long pos;
    public static final int SUCCESS = 0;
    public static final String FAIL = "1";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final HttpClientBuilder httpClientBuilder = HttpClients
        .custom()
        .disableRedirectHandling();
    private CloseableHttpClient httpClient;
    private static final String UPLOAD_URL_PATTERN = "%s/copy/upload";
    private static final String COMMIT_PATTERN = "%s/copy/query";
    private static final Pattern COMMITTED_PATTERN = Pattern.compile("errCode = 2, detailMessage = No files can be copied, matched (\\d+) files, " + "filtered (\\d+) files because files may be loading or loaded");


    public SelectdbCopyIntoObserver(Keys options) {
        this.options = options;
        this.httpClient = httpClientBuilder.build();

    }

    public void streamLoad(WriterTuple data) throws Exception {
        String host = getLoadHost();
        if (host == null) {
            throw new RuntimeException("load_url cannot be empty, or the host cannot connect.Please check your configuration.");
        }
        String loadUrl = String.format(UPLOAD_URL_PATTERN, host);
        String uploadAddress = getUploadAddress(loadUrl, data.getLabel());
        put(uploadAddress, data.getLabel(), addRows(data.getRows(), data.getBytes().intValue()));
        executeCopy(host,data.getLabel());

    }

    private String getUploadAddress(String loadUrl, String fileName) throws IOException {
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        putBuilder.setUrl(loadUrl)
            .addFileName(fileName)
            .addCommonHeader()
            .setEmptyEntity()
            .baseAuth(options.getUsername(), options.getPassword());
        CloseableHttpResponse execute = httpClientBuilder.build().execute(putBuilder.build());
        int statusCode = execute.getStatusLine().getStatusCode();
        String reason = execute.getStatusLine().getReasonPhrase();
        if (statusCode == 307) {
            Header location = execute.getFirstHeader("location");
            String uploadAddress = location.getValue();
            LOG.info("redirect to s3:{}", uploadAddress);
            return uploadAddress;
        } else {
            HttpEntity entity = execute.getEntity();
            String result = entity == null ? null : EntityUtils.toString(entity);
            LOG.error("Failed get the redirected address, status {}, reason {}, response {}", statusCode, reason, result);
            throw new RuntimeException("Could not get the redirected address.");
        }

    }

    private byte[] addRows(List<byte[]> rows, int totalBytes) {
        if (Keys.StreamLoadFormat.CSV.equals(options.getStreamLoadFormat())) {
            Map<String, Object> props = (options.getLoadProps() == null ? new HashMap<>() : options.getLoadProps());
            byte[] lineDelimiter = DelimiterParser.parse((String) props.get("file.line_delimiter"), "\n").getBytes(StandardCharsets.UTF_8);
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + rows.size() * lineDelimiter.length);
            for (byte[] row : rows) {
                bos.put(row);
                bos.put(lineDelimiter);
            }
            return bos.array();
        }

        if (Keys.StreamLoadFormat.JSON.equals(options.getStreamLoadFormat())) {
            ByteBuffer bos = ByteBuffer.allocate(totalBytes + (rows.isEmpty() ? 2 : rows.size() + 1));
            bos.put("[".getBytes(StandardCharsets.UTF_8));
            byte[] jsonDelimiter = ",".getBytes(StandardCharsets.UTF_8);
            boolean isFirstElement = true;
            for (byte[] row : rows) {
                if (!isFirstElement) {
                    bos.put(jsonDelimiter);
                }
                bos.put(row);
                isFirstElement = false;
            }
            bos.put("]".getBytes(StandardCharsets.UTF_8));
            return bos.array();
        }
        throw new RuntimeException("Failed to join rows data, unsupported `file.type` from copy into properties:");
    }

    public void put(String loadUrl, String fileName, byte[] data) throws IOException {
        LOG.info(String.format("Executing upload file to: '%s', size: '%s'", loadUrl, data.length));
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        putBuilder.setUrl(loadUrl)
            .addCommonHeader()
            .setEntity(new InputStreamEntity(new ByteArrayInputStream(data)));
        CloseableHttpResponse response = httpClient.execute(putBuilder.build());
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            String result = response.getEntity() == null ? null : EntityUtils.toString(response.getEntity());
            LOG.error("upload file {} error, response {}", fileName, result);
            throw new SelectdbWriterException("upload file error: " + fileName,true);
        }
    }

    private String getLoadHost() {
        List<String> hostList = options.getLoadUrlList();
        long tmp = pos + hostList.size();
        for (; pos < tmp; pos++) {
            String host = new StringBuilder("http://").append(hostList.get((int) (pos % hostList.size()))).toString();
            if (checkConnection(host)) {
                return host;
            }
        }
        return null;
    }

    private boolean checkConnection(String host) {
        try {
            URL url = new URL(host);
            HttpURLConnection co = (HttpURLConnection) url.openConnection();
            co.setConnectTimeout(5000);
            co.connect();
            co.disconnect();
            return true;
        } catch (Exception e1) {
            e1.printStackTrace();
            return false;
        }
    }


    /**
     * execute copy into
     */
    public void executeCopy(String hostPort, String fileName) throws IOException{
        long start = System.currentTimeMillis();
        CopySQLBuilder copySQLBuilder = new CopySQLBuilder(options, fileName);
        String copySQL = copySQLBuilder.buildCopySQL();
        LOG.info("build copy SQL is {}", copySQL);
        Map<String,String> params = new HashMap<>();
        params.put("sql", copySQL);
        if(StringUtils.isNotBlank(options.getClusterName())){
            params.put("cluster",options.getClusterName());
        }
        HttpPostBuilder postBuilder = new HttpPostBuilder();
        postBuilder.setUrl(String.format(COMMIT_PATTERN, hostPort))
            .baseAuth(options.getUsername(), options.getPassword())
            .setEntity(new StringEntity(OBJECT_MAPPER.writeValueAsString(params)));

        CloseableHttpResponse response = httpClient.execute(postBuilder.build());
        final int statusCode = response.getStatusLine().getStatusCode();
        final String reasonPhrase = response.getStatusLine().getReasonPhrase();
        String loadResult = "";
        if (statusCode != 200) {
            LOG.warn("commit failed with status {} {}, reason {}", statusCode, hostPort, reasonPhrase);
            throw new SelectdbWriterException("commit error with file: " + fileName,true);
        } else if (response.getEntity() != null){
            loadResult = EntityUtils.toString(response.getEntity());
            boolean success = handleCommitResponse(loadResult);
            if(success){
                LOG.info("commit success cost {}ms, response is {}", System.currentTimeMillis() - start, loadResult);
            }else{
                throw new SelectdbWriterException("commit fail",true);
            }
        }
    }

    public boolean handleCommitResponse(String loadResult) throws IOException {
        BaseResponse<CopyIntoResp> baseResponse = OBJECT_MAPPER.readValue(loadResult, new TypeReference<BaseResponse<CopyIntoResp>>(){});
        if(baseResponse.getCode() == SUCCESS){
            CopyIntoResp dataResp = baseResponse.getData();
            if(FAIL.equals(dataResp.getDataCode())){
                LOG.error("copy into execute failed, reason:{}", loadResult);
                return false;
            }else{
                Map<String, String> result = dataResp.getResult();
                if(!result.get("state").equals("FINISHED") && !isCommitted(result.get("msg"))){
                    LOG.error("copy into load failed, reason:{}", loadResult);
                    return false;
                }else{
                    return true;
                }
            }
        }else{
            LOG.error("commit failed, reason:{}", loadResult);
            return false;
        }
    }

    public static boolean isCommitted(String msg) {
        return COMMITTED_PATTERN.matcher(msg).matches();
    }


    public void close() throws IOException {
        if (null != httpClient) {
            try {
                httpClient.close();
            } catch (IOException e) {
                LOG.error("Closing httpClient failed.", e);
                throw new RuntimeException("Closing httpClient failed.", e);
            }
        }
    }
}
