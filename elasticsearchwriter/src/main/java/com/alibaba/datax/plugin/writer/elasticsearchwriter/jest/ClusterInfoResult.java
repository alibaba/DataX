package com.alibaba.datax.plugin.writer.elasticsearchwriter.jest;

import com.google.gson.Gson;
import io.searchbox.client.JestResult;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClusterInfoResult extends JestResult {

	private static final Pattern FIRST_NUMBER = Pattern.compile("\\d");

	private static final int SEVEN = 7;

	public ClusterInfoResult(Gson gson) {
		super(gson);
	}

	public ClusterInfoResult(JestResult source) {
		super(source);
	}

	/**
	 * 判断es集群的部署版本是否大于7.x
	 * 大于7.x的es对于Index的type有较大改动，需要做额外判定
	 * 对于7.x与6.x版本的es都做过测试，返回符合预期;5.x以下版本直接try-catch后返回false，向下兼容
	 * @return
	 */
	public Boolean isGreaterOrEqualThan7() throws Exception {
		// 如果是没有权限，直接返回false，兼容老版本
		if (responseCode == 403) {
			return false;
		}
		if (!isSucceeded) {
			throw new Exception(getJsonString());
		}
		try {
			String version = jsonObject.getAsJsonObject("version").get("number").toString();
			Matcher matcher = FIRST_NUMBER.matcher(version);
			matcher.find();
			String number = matcher.group();
			Integer versionNum = Integer.valueOf(number);
			return versionNum >= SEVEN;
		} catch (Exception e) {
			//5.x 以下版本不做兼容测试，如果返回json格式解析失败，有可能是以下版本，所以认为不大于7.x
			return false;
		}
	}
}
