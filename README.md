* 功能
1. 存储设备支持内存, redis, mysql, redis+mysql
2. 支持实体自动落地
3. 实体可设置为只读, 防止误写操作
4. dbmgr服务管理多个mysqld和redisd服务负载

* 落地规则
1. 如果实体同时支持mysql和redis, 初始加载数据,先从redis获取,redis没有数据再从MySQL获取. insert/delete先执行mysql再执行redis(执行redis出错不影响加载,mysql更严格), update先执行redis再执行mysql
2. single_entity只创建一次数据, 不删除数据
3. 删除和创建数据都是立即同步, 更新数据不会立即同步,需要外部调用flush进行同步 
