---
layout: post
title:  "记一次Spark程序遇到NoSuchMethodError后的解决过程"
date:   2021-03-29 01:36:40 +0800
categories: jekyll update
---

当我需要开发 Spark 作业时，我会使用组里一套封装好的 Spark 作业框架作为基础。框架会处理包括 Spark、Hive 等基础库的依赖管理，而我只需要关注计算逻辑的实现。然而最近，我需要在一个不同的 Spark 版本上开发一个作业，因此我决定绕过作业框架，自己配置整个项目的构建，包括处理基础库的依赖管理。

可当我写完代码，兴冲冲地将作业提交到集群上时，DUANG！程序崩了：
```console
Exception in thread "main" java.lang.NoSuchMethodError: org.apache.hadoop.hive.ql.log.PerfLogger.getPerfLogger()Lorg/apache/hadoop/hive/ql/log/PerfLogger;
// balabala....
```

通常，这类错误是对同一依赖的版本冲突导致的。比如，程序依赖的两个中间库 A，B 又都依赖了底层库 C 的两个不兼容的版本，而构建工具在构建时选择了其中一个版本，就会导致另一个中间库在调用 C 时发生错误。因此，我准备先找到出错的 `PerfLogger` 属于哪个中间库，再看看这个中间库在项目中是被如何依赖的。

> warning ""
> 实际上，由于 Spark 程序执行时的依赖既可能来源于用户自己构建的 jar，也可能来源于作业提交时通过诸如 `spark.jars`，`spark.yarn.archive` 等参数额外传入的 jar。因此，版本冲突未必发生在用户 jar 内部，也可能发生在用户 jar 和额外传入 jar 之间。在这个案例里，我其实做了额外的工作排除了这一可能。

找到一个类所属的 jar 可以通过下面两个方法:
1. 如果使用 IntelliJ，可以在 Search Everywhere（即快捷键 double shift）中搜索该类名；
2. 如果使用 Maven，可以通过 Maven Dependency Plugin 收集所有依赖的 jar，再对每个 jar 检查是否包含这个类。具体过程如下：


```bash
$ mvn dependency:copy-dependencies -DoutputDirectory=target/libs
$ cd target/libs
$ for j in `ls *.jar`; do jar tf $j | grep -qe "PerfLogger" && echo $j; done
hive-common-1.1.0-cdh5.7.3.jar
hive-exec-1.2.1.spark2.jar
```

好家伙，居然在两个 jar 中都搜到了这个类。看来这次的问题不是同一个 jar 的版本冲突，而是两个不同 jar 之间的冲突。于是，我分别验证了这两个 jar 中的 `PerfLogger` 的类信息：

```bash
# 检查 hive-common-1.1.0-cdh5.7.3.jar 中的 PerfLogger
$ mkdir hive-common && cd hive-common
$ jar xf ../hive-common-1.1.0-cdh5.7.3.jar
$ javap org/apache/hadoop/hive/ql/log/PerfLogger.class
Compiled from "PerfLogger.java"
public class org.apache.hadoop.hive.ql.log.PerfLogger {
  public static final java.lang.String ACQUIRE_READ_WRITE_LOCKS;
  public static final java.lang.String COMPILE;
  ...
  public static org.apache.hadoop.hive.ql.log.PerfLogger getPerfLogger(org.apache.hadoop.hive.conf.HiveConf, boolean);
  ...
}

# 对 hive-exec-1.2.1.spark2.jar 执行同样的操作
Compiled from "PerfLogger.java"
public class org.apache.hadoop.hive.ql.log.PerfLogger {
  public static final java.lang.String ACQUIRE_READ_WRITE_LOCKS;
  public static final java.lang.String COMPILE;
  ...
  public static org.apache.hadoop.hive.ql.log.PerfLogger getPerfLogger();
  public static org.apache.hadoop.hive.ql.log.PerfLogger getPerfLogger(boolean);
  ...
}
```

果然，在 `hive-common-1.1.0-cdh5.7.3.jar` 中的 `PerfLogger` 并没有无参的 `getPerfLogger` 方法，而 Maven 选择将这个 jar 中的 `PerfLogger` 打进了最终的用户 jar 中，从而导致了最开始错误。那么，为啥 Maven 会选择使用 `hive-common-1.1.0-cdh5.7.3.jar` 中的类呢？其实，Maven 项目存在一个 build classpath，Maven 在构建时会优先使用 build class 更靠前的 jar 中的类。

使用 Maven Dependency Plugin 可以查看项目 build classpath：

```bash
$ mvn dependency:build-classpath '-Dmdep.pathSeparator=${line.separator}'
...
[INFO] --- maven-dependency-plugin:2.8:build-classpath (default-cli) @ pipeline ---
[INFO] Dependencies classpath:
/home/tao.wang/.m2/repository/org/scala-lang/scala-library/2.11.12/scala-library-2.11.12.jar
/home/tao.wang/.m2/repository/com/google/guava/guava/30.1.1-jre/guava-30.1.1-jre.jar
...
/home/tao.wang/.m2/repository/org/apache/hive/hive-common/1.1.0-cdh5.7.3/hive-common-1.1.0-cdh5.7.3.jar
...
/home/tao.wang/.m2/repository/org/spark-project/hive/hive-exec/1.2.1.spark2/hive-exec-1.2.1.spark2.jar
...
```

果然，`hive-common-1.1.0-cdh5.7.3.jar` 在 build classpath 中是更靠前的一个。

接下来，我们需要知道这两个 jar 在项目中是怎样被依赖的，还是通过 Maven Dependency Plugin：

```bash
$ mvn dependency:tree -Dincludes=org.apache.hive:hive-common
...
[INFO] --- maven-dependency-plugin:2.8:tree (default-cli) @ pipeline ---
[INFO] com.xxx.yyy.zzzpoc:pipeline:jar:1.0-SNAPSHOT
[INFO] \- com.xxx.yyy.database:presto:jar:0.0.4:compile
[INFO]    \- org.apache.hive:hive-jdbc:jar:1.1.0-cdh5.7.3:compile
[INFO]       \- org.apache.hive:hive-common:jar:1.1.0-cdh5.7.3:compile
...

$ mvn dependency:tree -Dincludes=org.spark-project.hive:hive-exec
...
[INFO] --- maven-dependency-plugin:2.8:tree (default-cli) @ pipeline ---
[INFO] com.xxx.yyy.zzzpoc:pipeline:jar:1.0-SNAPSHOT
[INFO] \- org.apache.spark:spark-hive_2.11:jar:2.3.4:provided
[INFO]    \- org.spark-project.hive:hive-exec:jar:1.2.1.spark2:provided
...
```

这个 `com.xxx.yyy.database:presto` 其实也是组里为使用公司 Presto 集群封装的一个 jar，但为啥有 Hive 的依赖？看来封装得有点问题。。

最后，怎么解决这个问题呢？一种思路是调整依赖在 build classpath 中的顺序，让 Maven 选择正确的 jar 来构建，但这个方法不够健壮。另外，由于 `hive-exec-1.2.1.spark2.jar` 是一个 provided dependency，这样做依然不能使该 jar 中的类被打入到最终 jar 中。

因此，更合适的方法是将 `hive-common-1.1.0-cdh5.7.3.jar` 排除在 dependency tree 之外。

```xml
<dependency>
    <groupId>com.xxx.yyy.database</groupId>
    <artifactId>presto</artifactId>
    <version>0.0.4</version>
    <exclusions>
        <exclusion>
            <groupId>org.apache.hive</groupId>
            <artifactId>hive-common</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

改完之后，程序就能顺利跑过啦！
