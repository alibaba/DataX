package com.alibaba.datax.common.plugin;

import com.alibaba.datax.common.util.Configuration;

public interface Pluginable {

  String getDeveloper();

  String getDescription();

  void setPluginConf(Configuration pluginConf);

  /**
   * 初始化插件
   */
  void init();

  /**
   * 销毁插件
   */
  void destroy();

  String getPluginName();

  Configuration getPluginJobConf();

  Configuration getPeerPluginJobConf();

  String getPeerPluginName();

  void setPluginJobConf(Configuration jobConf);

  void setPeerPluginJobConf(Configuration peerPluginJobConf);

  void setPeerPluginName(String peerPluginName);

}
