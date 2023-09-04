DwdTradeCartAdd

-> 交易域加购事实表

// 流执行环境
StreamExecutionEnvironment env = new StreamExecutionEnvironment.getExecutionEnvironment();

// 设置并行度为4
env.setParallelism(4);

// 表执行环境
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// 检查点设置
// 开启检查点, 设置检查点时间为3秒, 检查点模式为精准一次
env.enableCheckpointing(3000L, CheckpointMode.EXACTLY_ONCE);
// 设置检查点超时时间为1min
env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
// 设置两次检查点之间的最小时间间隔为3秒
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
// 设置取消任务时, 仍然保留外部检查点
env.getcheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
// 设置重启策略为失败率重启, 时效为1天, 一天重启最大次数3次, 没两次间隔为3s
env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.seconds(3)));
// 设置状态后端
// 设置状态在taskmanager内存中的存储形式为hashMap
env.setStateBackend(new HashMapStateBackend());
// 设置检查点远程存储路径
env.getCheckpointConfig().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");
// 设置操作hdfs的用户
System.setProperty("HADOOP_USER_NAME", baiyao);

// 从kafka的topic_db中读取数据, 创建动态表
// 创建动态表topic_db, 表中有从kafka读到的
// 数据库名、表名、操作类型、maxwell读取数据时的时间戳、操作之前的数据、最新数据、当前的处理时间

// with中需要指定相关属性:
// kafka连接器、从哪个kafka的topic读数据、kafka集群地址、消费者组、初始化扫描方式、读取到的数据格式 
tableEnv
    .executeSql(
        "create table topic_db (\n" +
        "    `database` String,\n" +
        "    `table` String,\n" +
        "    `type` String,\n" +
        "    `ts` String,\n" +
        "    `old` MAP<String, String>,\n" +
        "    `data` MAP<String, String>,\n" +
        "    proc_time as proctime() \n" +
        ") with (\n" +
        "    'connector' = 'kafka',\n" +
        "    'topic' = 'topic_db',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'properties.group.id' = 'dwd_trade_cart_add_group',\n" +
        "    'scan.startup.mode' = 'group-offsets',\n" +
        "    'format' = 'json'\n" +
        ")"
    )
    // 过滤出加购数据
    // cart_info表中的insert类型是加购操作, 加购数量就是sku_num
    // 另外cart_info中update操作也包含加购, 前提是新的sku_num比旧的sku_num数量多, 两者相减就是加购数量
    .sqlQuery(
        "select \n" +
        "    data['id'] id,\n" +
        "    data['user_id'] user_id,\n" +
        "    data['sku_id'] sku_id,\n" +
        "    data['source_type'] source_type,\n" +
        "    ts,\n" +
        "    proc_time,\n" +
        "    if (\n" +
        "        `type` = 'insert', data['sku_num'], CAST(\n" +
        "            (cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string\n" +
        "        )\n" +
        "    ) sku_num \n" +
        "from topic_db\n" +
        "where `table` = `cart_info` and\n" +
        "      (\n" +
        "        `type` = 'insert' or \n" +
        "        (\n" +
        "            `type` = 'update' and `old`['sku_num'] is not null and CAST(data['sku_num'] as int) > CAST(`old`['sku_num'] as int)\n" +
        "        )\n" +
        "      )\n";
    );

    -> Table cartTable

// 将上面查到的加购表注册为临时视图
tableEnv
    .createTemporaryView("cart_table", cartTable);

// 从Mysql数据库中获取字典表数据
// lookupJoin底层实现与普通的内外连接完全不一样
// lookupJoin底层并不会维护两个状态来存两表的数据, 它以左表为驱动, 当左表数据来得时候, 会请求到外部系统查询右表数据进行连接, 右表变化来一条数据的时候, 是不会到左表进行查询的 

// 由于左表来一次就需要与mysql数据库连接查询一次, 过于频繁, 所以此时就需要将右表进行缓存
// 'lookup.cache.max-rows' = '200' 当第201条数据过来的时候, 就会替换最老的第一条数据
// 'lookup.cache.ttl' = '1 hour' 记录每一条数据的缓存时间为1h, 1h后过期
// 1h内如果原数据库数据有更改, 缓存数据不会被更新

// 什么时候适合使用lookupJoin: 当需要使用外部系统表数据来补充左表数据的时候就需要使用lookupJoin, 另外就是这张表的数据量不应该太大, 太大的话缓存起来有压力, 另一个就是表不应该经常发生变化

tableEnv
    .executeSql(
        "create table base_dic (\n" +
        "    dic_code String,\n" +
        "    dic_name String,\n" +
        "    primary key (dic_code) not enforced\n" +
        ") with (\n" +
        "    'connector' = 'jdbc',\n" +
        "    'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
        "    'url' = 'jdbc:mysql://zoo1:3306/gmall',\n" +
        "    'table-name' = 'base_dic',\n" +
        "    'lookup.cache.max-rows' = '200',\n" +
        "    'lookup.cache.ttl' = '1 hour',\n" +
        "    'username' = 'root',\n" +
        "    'password' = '123456'\n" +
        ")"
    );

// 将字典表退化到加购事实表中
// 这里的base_dic使用lookupJoin, system_time处理时间字段需要根据cart_table的proc_time进行指定

Table resultTable = TableEnv
    .sqlQuery(
        "select\n" +
        "    cartTable.id,\n" +
        "    cartTable.user_id,\n" +
        "    carttable.sku_id,\n" +
        "    cartTable.sku_num,\n" +
        "    cartTable.source_type,\n" +
        "    dic.dic_name,\n" +
        "    cartTable.ts\n" +
        "from cart_table as cartTable\n" +
        "join base_dic for system_time as of cartTable.proc_time as dic\n" +
        "on cartTable.source_type = dic.dic_code"
    );

// 为关联后的表创建临时视图
tableEnv
    .createTemporaryView("res_table", resultTable);

// 创建动态表和要写入的kafka主题建立映射关系
tableEnv
    .executeSql(
        "create table dwd_trade_cart_add (\n" +
        "    id String,\n" +
        "    user_id String,\n" +
        "    sku_id String,\n" +
        "    sku_num String,\n" +
        "    source_type_code String,\n" +
        "    source_type_name String,\n" +
        "    ts String,\n" +
        "    primary key (id) not enforced\n" +
        ") with (\n" +
        "    'connector' = 'upsert-kafka',\n" +
        "    'topic' = 'dwd_trade_cart_add',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'key.format' = 'json',\n" +
        "    'value.format' = 'json'\n" +
        ")"
    );

// 将关联的结果写到kafka主题中
tableEnv
    .executeSql(
        "insert into dwd_trade_cart_add select * from res_table"
    );
