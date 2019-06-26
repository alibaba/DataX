odpsreader项目的pom
odpswriter项目的pom
<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-sdk-core</artifactId>
    <version>0.20.7-public</version>
</dependency>

otsstreamreader项目的pom
<dependency>
    <groupId>com.aliyun.openservices</groupId>
    <artifactId>tablestore-streamclient</artifactId>
    <version>1.0.0</version>
</dependency>


mvn install:install-file -Dfile=pentaho-aggdesigner-algorithm-5.1.5-jhyde.jar -DgroupId=org.pentaho -DartifactId=pentaho-aggdesigner-algorithm -Dversion=5.1.5-jhyde -Dpackaging=jar

mvn install:install-file -Dfile=eigenbase-properties-1.1.4.jar -DgroupId=eigenbase -DartifactId=eigenbase-properties -Dversion=1.1.4 -Dpackaging=jar