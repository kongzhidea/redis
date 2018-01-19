

### 依赖版本
```
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>2.6.2</version>
</dependency>

2.1.0 采用的连接池为 common-pool

2.6.2 采用的连接池为 common-pool2
```

#### 2.6.2 版本
* maxTotal：链接池中最大连接数，默认值8。
  * commons-pool1 中maxActive
* maxIdle：连接池中最大空闲的连接数,默认为8
* minIdle: 连接池中最少空闲的连接数,默认为0
* maxWaitMillis：当连接池资源耗尽时，调用者最大阻塞的时间，超时将跑出异常。单位，毫秒数;默认为-1.表示永不超时. 默认值 -1。
  * maxWait：commons-pool1中
* testOnBorrow：向调用者输出“链接”资源时，是否检测是有有效，如果无效则从连接池中移除，并尝试获取继续获取。默认为false。建议保持默认值.
* testOnReturn：默认值false
* testWhileIdle：向调用者输出“链接”对象时，是否检测它的空闲超时；默认为false。如果“链接”空闲超时，将会被移除；建议保持默认值。默认值false
whenExhaustedAction: 当“连接池”中active数量达到阀值时，即“链接”资源耗尽时，连接池需要采取的手段, 默认为1：
  * 0：抛出异常
  * 1：阻塞，直到有可用链接资源
  * 2：强制创建新的链接资源

