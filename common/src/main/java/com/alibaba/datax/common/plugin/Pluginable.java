package com.alibaba.datax.common.plugin;

import com.alibaba.datax.common.util.Configuration;

public interface Pluginable {
	String getDeveloper();

    String getDescription();

    void setPluginConf(Configuration pluginConf);

	void init();

	void destroy();

    String getPluginName();

    Configuration getPluginJobConf();

    Configuration getPeerPluginJobConf();

    public String getPeerPluginName();

    void setPluginJobConf(Configuration jobConf);

    void setPeerPluginJobConf(Configuration peerPluginJobConf);

    public void setPeerPluginName(String peerPluginName);

}
