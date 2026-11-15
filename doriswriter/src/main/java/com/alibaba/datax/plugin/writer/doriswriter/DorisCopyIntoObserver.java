package com.alibaba.datax.plugin.writer.doriswriter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class DorisCopyIntoObserver {
    private static final Logger LOG = LoggerFactory.getLogger(DorisCopyIntoObserver.class);

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
    private static final Pattern COMMITTED_PATTERN = Pattern.compile("errCode = 2, detailMessage = No files can be copied.*");

    public DorisCopyIntoObserver(Keys options) {
        this.options = options;
        this.httpClient = httpClientBuilder.build();

    }

    public void streamLoad(WriterTuple data) throws Exception {
        String host = DorisUtil.getLoadHost(options);
        String loadUrl = String.format(UPLOAD_URL_PATTERN, host);
        String uploadAddress = getUploadAddress(loadUrl, data.getLabel());
        put(uploadAddress, data.getLabel(), DorisUtil.addRows(options, data.getRows(), data.getBytes().intValue()));
        executeCopy(host, data.getLabel());

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


    public void put(String loadUrl, String fileName, byte[] data) throws IOException {
        LOG.info(String.format("Executing upload file to: '%s', size: '%s'", loadUrl, data.length));
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        putBuilder.setUrl(loadUrl)
            .addCommonHeader()
            .setEntity(new ByteArrayEntity(data));
        CloseableHttpResponse response = httpClient.execute(putBuilder.build());
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            String result = response.getEntity() == null ? null : EntityUtils.toString(response.getEntity());
            LOG.error("upload file {} error, response {}", fileName, result);
            throw new DorisWriterExcetion("upload file error: " + fileName, true);
        }
    }


    /**
     * execute copy into
     */
    public void executeCopy(String hostPort, String fileName) throws IOException {
        long start = System.currentTimeMillis();
        CopySQLBuilder copySQLBuilder = new CopySQLBuilder(options, fileName);
        String copySQL = copySQLBuilder.buildCopySQL();
        LOG.info("build copy SQL is {}", copySQL);
        Map<String, String> params = new HashMap<>();
        params.put("sql", copySQL);
        if (StringUtils.isNotBlank(options.getClusterName())) {
            params.put("cluster", options.getClusterName());
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
            throw new DorisWriterExcetion("commit error with file: " + fileName, true);
        } else if (response.getEntity() != null) {
            loadResult = EntityUtils.toString(response.getEntity());
            boolean success = handleCommitResponse(loadResult);
            if (success) {
                LOG.info("commit success cost {}ms, response is {}", System.currentTimeMillis() - start, loadResult);
            } else {
                LOG.error("commit error with status {}, reason {}, response {}", statusCode, reasonPhrase, loadResult);
                String copyErrMsg = String.format("commit error, status: %d, reason: %s, response: %s, copySQL: %s",
                    statusCode, reasonPhrase, loadResult, copySQL);
                throw new DorisWriterExcetion(copyErrMsg, true);
            }
        }
    }

    public boolean handleCommitResponse(String loadResult) throws IOException {
        BaseResponse baseResponse = OBJECT_MAPPER.readValue(loadResult, new TypeReference<BaseResponse>() {
        });
        if (baseResponse.getCode() == SUCCESS) {
            CopyIntoResp dataResp = OBJECT_MAPPER.convertValue(baseResponse.getData(), CopyIntoResp.class);
            if (FAIL.equals(dataResp.getDataCode())) {
                LOG.error("copy into execute failed, reason:{}", loadResult);
                return false;
            } else {
                Map<String, String> result = dataResp.getResult();
                if (DorisUtil.isNullOrEmpty(result) || !result.get("state").equals("FINISHED") && !isCommitted(result.get("msg"))) {
                    LOG.error("copy into load failed, reason:{}", loadResult);
                    return false;
                } else {
                    return true;
                }
            }
        } else {
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
