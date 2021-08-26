## 快速介绍

​	**引入插件机制目的**：神策分析支持可变事件，在```sahistorywriter```写插件的基础上是无法实现通用的实现方案，所以提供插件机制以支持可变事件，当然不仅仅是可变事件，其他定制化开发也是可以的，神策内部已实现使用redis实现通用可变事件，如需要，请联系神策开发人员。

## **实现原理**

神策写插件会实例化插件类，并调用instance方法获取到BasePlugin.SAPlugin插件实例，然后调用SAPlugin的process方法（经过转换器转换后的值会被传递到该方法中，空值将会被丢弃）。

## **神策写插件插件规范**

- ​	引入common依赖

  ```xml
  <dependency>
      <groupId>cn.sensorsdata</groupId>
      <artifactId>plugin-sa-history-datax-writer-common-plugin</artifactId>
      <version>1.0-SNAPSHOT</version>
  </dependency>
  ```

- 编写代码

  继承com.alibaba.BasePlugin类，重写instance方法（配置文件中plugin.param的配置项会被传递到该方法中），以及定义内部类继承com.alibaba.BasePlugin的内部类BasePlugin.SAPlugin，重写process方法。

- 部署插件

  将插件连同依赖一起打包生成jar包，在datax的```sahistorywriter```插件下新建plugin文件夹，然后再新建一个放置该插件的文件夹，命名无要求，配置文件中```plugin.name```参数为该文件夹名，最后将生成的jar包放置到该文件夹下。


