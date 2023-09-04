DwdTradeOrderPreProcess

-> 交易域订单预处理表

// 流环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 并行度为4
env.setParallelism(4);
// 表环境
StreamTableEnvironment tableEnv = StreamTableExecutionEnvironment.create(env);
// 设置状态的TTL
tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15 * 60 + 5));

// 开启检查点, 设置检查点时间为3秒, 检查点模式为精准一次
env.enableCheckpointing(3000L, CheckpointMode.EXACTLY_ONCE);
// 检查点超时时间为1min
env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
// 两次检查点之间的最小间隔时间为3秒
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
// 取消任务的时候设置保留检查点
env.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
// 重启策略设为失败率重启, 有效时间为1天, 一天三次, 没两次间隔为1s
env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.seconds(1)));
// 设置状态后端
// 设置状态在taskmanager的堆内存中的存储形式为hashmap
env.setStateBackend(new HashMapStateBackend());
// 设置远端存储路径
env.getCheckpointConfig().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");
// 设置操作hdfs的用户
System.setProperty("HADOOP_USER_NAME", baiyao);

// 从kafka的topic_db中读取数据并创建动态表
tableEnv
    .executeSql(
        "create table topic_db (\n" +
        "    `database` String,\n" +
        "    `table` String,\n" +
        "    `type` String,\n" +
        "    `ts` String,\n" +
        "    `old` MAP<String, String>,\n" +
        "    `data` MAP<String, String>,\n" +
        "    proc_time as proctime()\n" +
        ") with (\n" +
        "    'connector' = 'kafka',\n" +
        "    'topic' = 'topic_db',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'properties.group.id' = 'dwd_trade_order_pre_process_group',\n" +
        "    'scan.startup.mode' = 'group-offsets',\n" +
        "    'format' = 'json'\n" +
        ")"
    );

// 过滤出订单明细数据

Table orderDetail = tableEnv
    .sqlQuery(
        "select\n" +
        "    data['id'] id,\n" +
        "    data['order_id'] order_id,\n" +
        "    data['sku_id'] sku_id,\n" +
        "    data['sku_name'] sku_name,\n" +
        "    data['create_time'] create_time,\n" +
        "    data['source_id'] source_id,\n" +
        "    data['source_type'] source_type,\n" +
        "    data['sku_num'] sku_num,\n" +
        "    cast(cast(data['sku_num'] as decimal(16, 2)) * cast(data['order_price'] as decimal(16, 2)) as String) split_original_amount,\n" +
        "    data['split_total_amount'] split_total_amount,\n" +
        "    data['split_activity_amount'] split_activity_amount,\n" +
        "    data['split_coupon_amount'] split_coupon_amount,\n" +
        "    ts od_ts,\n" +
        "    proc_time\n" +
        "from `topic_db` where `table` = 'order_detail' and `type` = 'insert'"
    );

tableEnv
    .createTemporaryView("order_detail", orderDetail);

// 过滤出订单数据
Table orderInfo = tableEnv
    .sqlQuery(
        "select\n" +
        "    data['id'] id,\n" +
        "    data['user_id'] user_id,\n" +
        "    data['province_id'] province_id,\n" +
        "    data['operate_time'] operate_time,\n" +
        "    data['order_status'] order_status,\n" +
        "    `type`,\n" +
        "    `old`,\n" +
        "    ts oi_ts\n" +
        "from `topic_db`\n" +
        "where `table` = 'order_info' and (`type` = 'insert' or `type` = 'update')"
    );

tableEnv
    .createTemporaryView("order_info", orderInfo);

// 过滤出订单明细活动数据
Table orderDetailActivity = tableEnv
    .sqlQuery(
        "select\n" +
        "    data['order_detail_id'] order_detail_id,\n" +
        "    data['activity_id'] activity_id,\n" +
        "    data['activity_rule_id'] activity_rule_id\n" +
        "from `topic_db`\n" +
        "where `table` = 'order_detail_activity'\n" +
        "and `type` = 'insert'"
    );

tableEnv
    .createTemporaryView("order_detail_activity", orderDetailActivity);

// 过滤出订单明细优惠券数据
Table orderDetailCoupon = tableEnv
    .sqlQuery(
        "select\n" +
        "    data['order_detail_id'] order_detail_id,\n" +
        "    data['coupon_id'] coupon_id\n" +
        "from `topic_db`\n" +
        "where `table` = 'order_detail_coupon' and type = 'insert'"
    );

tableEnv
    .createTemporaryView("order_detail_coupon", orderDetailCoupon);

// 从Mysql数据库读取字典维度数据创建动态表
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
    )

// 关联上述5张表
Table resultTable = TableEnv
    // current_row_timestamp()是系统内置函数, 获取的是当前5张表关联完毕之后的系统时间
    .sqlQuery(
        "select\n" +
        "    od.id,\n" +
        "    od.order_id,\n" +
        "    od.sku_id,\n" +
        "    od.sku_name,\n" +
        "    date_format(od.create_time, 'yyyy-MM-dd') date_id,\n" +
        "    od.create_time,\n" +
        "    od.source_id,\n" +
        "    od.source_type,\n" +
        "    od.sku_num,\n" +
        "    od.split_original_amount,\n" +
        "    od.split_activity_amount,\n" +
        "    od.split_coupon_amount,\n" +
        "    od.split_total_amount,\n" +
        "    od.od_ts,\n" +
        "\n" +
        "    oi.user_id,\n" +
        "    oi.province_id,\n" +
        "    date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id,\n" +
        "    oi.operate_time,\n" +
        "    oi.order_status,\n" +
        "    oi.`type`,\n" +
        "    oi.`old`,\n" +
        "    oi.oi_ts,\n" +
        "\n" +
        "    act.activity_id,\n" +
        "    act.activity_rule_id,\n" +
        "\n" +
        "    cou.coupon_id,\n" +
        "\n" +
        "    dic.dic_name source_type_name,\n" +
        "\n" +
        "    current_row_timestamp() row_op_ts\n" +
        "from order_detail od\n" +
        "join order_info oi on od.order_id = oi.id\n" +
        "left join order_detail_activity act on od.id = act.order_detail_id\n" +
        "left join order_detail_coupon cou on od.id = cou.order_detail_id\n" +
        "join `base_dic` for system_time as of od.proc_time as dic\n" +
        "on od.source_type = dic.dic_code"
    );

tableEnv
    .createTemporaryView("result_table", resultTable);

// 创建动态表和要写入的kafka主题进行映射
tableEnv
    .executeSql(
        "create table dwd_trade_order_pre_process (\n" +
        "    id string,\n" +
        "    order_id string,\n" +
        "    sku_id string,\n" +
        "    sku_name string,\n" +
        "    date_id string,\n" +
        "    create_time string,\n" +
        "    source_id string,\n" +
        "    source_type string,\n" +
        "    sku_num string,\n" +
        "    split_original_amount string,\n" +
        "    split_activity_amount string,\n" +
        "    split_coupon_amount string,\n" +
        "    split_total_amount string,\n" +
        "    od_ts string,\n" +
        "\n" +
        "    user_id string,\n" +
        "    province_id string,\n" +
        "    operate_date_id string,\n" +
        "    operate_time string,\n" +
        "    order_status string,\n" +
        "    `type` string,\n" +
        "    `old` map<string, string>,\n" +
        "    oi_ts string,\n" +
        "\n" +
        "    activity_id string,\n" +
        "    activity_rule_id string,\n" +
        "\n" +
        "    coupon_id string,\n" +
        "\n" +
        "    source_type_name string,\n" +
        "\n" +
        "    row_op_ts timestamp_ltz(3),\n" +
        "    primary key(id) not enforced\n" +
        ") with (\n" +
        "    'connector' = 'upsert-kafka',\n" +
        "    'topic' = 'dwd_trade_order_pre_process',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'key.format' = 'json',\n" +
        "    'value.format' = 'json'\n" +
        ")"
    );

tableEnv
    .executeSql(
        "insert into dwd_trade_order_pre_process select * from result_table"
    );