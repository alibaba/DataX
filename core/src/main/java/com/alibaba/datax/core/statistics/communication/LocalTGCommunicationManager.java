package com.alibaba.datax.core.statistics.communication;

import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang3.Validate;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地模式taskGroup的communication管理类
 */
public final class LocalTGCommunicationManager {

  private static Map<Integer, Communication> taskGroupCommunicationMap = new ConcurrentHashMap<>();

  /**
   * 根据tgId注册comm
   * 当task组在初始化的时候，都会向LocalTGCommunicationManager这里注册。// 这里只是简单保存到taskGroupCommunicationMap变量里
   * @param taskGroupId
   * @param communication
   */
  public static void registerTaskGroupCommunication(int taskGroupId, Communication communication) {
    taskGroupCommunicationMap.put(taskGroupId, communication);
  }

  /**
   * 获取（合并）tg里面所有的comm
   *
   * @return Communication
   */
  public static Communication getJobCommunication() {
    Communication communication = new Communication();
    communication.setState(State.SUCCEEDED);

    for (Communication taskGroupCommunication : taskGroupCommunicationMap.values()) {
      communication.mergeFrom(taskGroupCommunication);
    }
    return communication;
  }

  /**
   * 采用获取taskGroupId后再获取对应communication的方式，
   * 防止map遍历时修改，同时也防止对map key-value对的修改
   *
   * @return
   */
  public static Set<Integer> getTaskGroupIdSet() {
    return taskGroupCommunicationMap.keySet();
  }

  public static Communication getTaskGroupCommunication(int taskGroupId) {
    Validate.isTrue(taskGroupId >= 0, "taskGroupId不能小于0");
    return taskGroupCommunicationMap.get(taskGroupId);
  }


  /**
   * 根据tgId 将taskGroupCommunicationMap中没有的comm 插入
   * @param taskGroupId
   * @param comm
   */
  public static void updateTaskGroupCommunication(final int taskGroupId, final Communication comm) {
    Validate.isTrue(taskGroupCommunicationMap.containsKey(
        taskGroupId), String.format("taskGroupCommunicationMap中没有注册taskGroupId[%d]的Communication，" +
        "无法更新该taskGroup的信息", taskGroupId));
    taskGroupCommunicationMap.put(taskGroupId, comm);
  }

  public static void clear() {
    taskGroupCommunicationMap.clear();
  }

  public static Map<Integer, Communication> getTaskGroupCommunicationMap() {
    return taskGroupCommunicationMap;
  }
}