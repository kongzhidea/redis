## [redis官方网站](http://redis.cn/)

### redis客户端
* fastonosql
* phpRedisAdmin
* redis-cli 推荐

### [redis Java客户端使用](https://github.com/kongzhidea/redis)

### linux下redis安装部署
```


1、安装
tar -zxvf redis-2.8.7.tar.gz  
cd redis-2.8.7 
make

2、调整内存
如果内存情况比较紧张的话，需要设定内核参数：
该配置文件依据linux系统不同而不同，参考启动时候的提示
echo 1 > /proc/sys/vm/overcommit_memory

这里说一下这个配置的含义：
/proc/sys/vm/overcommit_memory
该文件指定了内核针对内存分配的策略，其值可以是0、1、2。
0，表示内核将检查是否有足够的可用内存供应用进程使用；如果有足够的可用内存，内存申请允许；否则，内存申请失败，并把错误返回给应用进程。
1，表示内核允许分配所有的物理内存，而不管当前的内存状态如何。
2，表示内核允许分配超过所有物理内存和交换空间总和的内存

3.redis.conf 配置文件
参考redis.conf，主要配置项
 dir  数据存储目录
 pidfile  运行时进程id文件，默认为 /var/run/redis.pid
 port 监听端口，默认为6379
 daemonize 是否以后台进程运行，默认为no
 requirepass 是否需要密码，如果不需要密码则注释此行
 maxmemory 申请最大内存，如：maxmemory 4g
 
 timeout 超时时间，默认为300（秒）
 loglevel 日志记录等级，有4个可选值，debug，verbose（默认值），notice，warning
 logfile 日志记录方式，默认值为stdout
 databases 可用数据库数，默认值为16，默认数据库为0
 save <seconds> <changes> 指出在多长时间内，有多少次更新操作，就将数据同步到数据文件。
 save可以多个条件配合，比如默认配置文件中的设置，就设置了三个条件。
	 save 900 1  900秒（15分钟）内至少有1个key被改变
	 save 300 10  300秒（5分钟）内至少有300个key被改变
	 save 60 10000  60秒内至少有10000个key被改变
 rdbcompression 存储至本地数据库时是否压缩数据，默认为yes
 dbfilename 本地数据库文件名，默认值为dump.rdb

4、启动服务
redis-2.8.7/src/redis-server
redis-server redis.conf   #启动服务

启动脚本：  端口，数据存储路径（要预先创建好），在配置文件中配置，
nohup redis-server  /data/redis/redis-3.0.5/conf/redis.conf  > log 2>&1 &

killall -9 redis-server     #关闭服务
redis-2.8.7/src/redis-cli shutdown  #关闭服务
redis-cli -p 6380 shutdown  #指定端口 关闭服务
 
redis-cli  (-a password) #连接客户端

redis-cli -h localhost -p 6379   #连接客户端 指定ip和端口

redis-cli -h localhost -p 6379 -a password  #连接客户端 指定ip和端口，指定密码，
与requirepass配合使用，也可以在redis-cli中使用 auth命令，如 auth password。


后台启动命令：
nohup redis-server  /data/redis/redis-3.0.5/conf/redis.conf  > log 2>&1 &
```

### 批量删除key
* redis-cli keys "keyword*" | xargs redis-cli del

