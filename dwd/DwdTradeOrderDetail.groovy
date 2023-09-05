DwdTradeOrderDetail

-> 交易域下单事务事实表

// 流处理环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 并行度为4
env.setParallelism(4);
// 表处理环境
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// 开启检查点, 设置检查点时间为3秒, 检查点模式为精准一次
env.enableCheckpointing(3000L, CheckpointMode.EXACTLY_ONCE);
// 设置检查点超时时间为1min
env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
// 设置两次检查点之间最小的时间间隔为3秒
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
// 设置取消任务之后仍然保留检查点
env.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
// 设置重启策略为失败率重启, 重启的次数为3, 有效时间为1天, 两次重启之间的时间间隔为3秒
env.setRestartStrategy.(RestartStrategies.failureRateRestart(3, Time.days(1), Time.seconds(3)));
// 设置状态后端
// 设置状态在taskmanager内存中的存储形式为hashmap
env.setStateBackend(new HashMapStateBackend());
// 设置检查点远程存储路径
env.getCheckpointConfig().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");
// 设置操作hdfs的用户
System.setProperty("HADOOP_USER_NAME", baiyao);

// 从kafka的订单预处理dwd_trade_order_pre_process主题中读取数据并创建动态表
tableEnv
    .executeSql(
        "create table dwd_trade_order_pre_process (\n" +
        "    id string,\n" +
        "    order_id string,\n" +
        "    user_id string,\n" +
        "    order_status string,\n" +
        "    sku_id string,\n" +
        "    sku_name string,\n" +
        "    province_id string,\n" +
        "    activity_id string,\n" +
        "    activity_rule_id string,\n" +
        "    coupon_id string,\n" +
        "    date_id string,\n" +
        "    create_time string,\n" +
        "    operate_date_id string,\n" +
        "    operate_time string,\n" +
        "    source_id string,\n" +
        "    source_type string,\n" +
        "    source_type_name string,\n" +
        "    sku_num string,\n" +
        "    split_original_amount string,\n" +
        "    split_activity_amount string,\n" +
        "    split_coupon_amount string,\n" +
        "    split_total_amount string,\n" +
        "    `type` string,\n" +
        "    `old` map<string, string>,\n" +
        "    od_ts string,\n" +
        "    oi_ts string,\n" +
        "    row_op_ts timestamp_ltz(3)\n" +
        ") with (\n" +
        "    'connector' = 'kafka',\n" +
        "    'topic' = 'dwd_trade_order_pre_process',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'properties.group.id' = 'dwd_trade_order_detail_group',\n" +
        "    'scan.startup.mode' = 'group-offsets',\n" +
        "    'format' = 'json'\n" +
        ")"
    );

// 过滤出下单明细数据: 订单表操作类型为 insert 的数据即为订单明细数据
Table filteredTable = tableEnv
    .sqlQuery(
        "select\n" +
        "    id,\n" +
        "    order_id,\n" +
        "    user_id,\n" +
        "    sku_id,\n" +
        "    sku_name,\n" +
        "    province_id,\n" +
        "    activity_id,\n" +
        "    activity_rule_id,\n" +
        "    coupon_id,\n" +
        "    date_id,\n" +
        "    create_time,\n" +
        "    source_id,\n" +
        "    source_type source_type_code,\n" +
        "    source_type_name,\n" +
        "    sku_num,\n" +
        "    split_original_amount,\n" +
        "    split_activity_amount,\n" +
        "    split_coupon_amount,\n" +
        "    split_total_amount,\n" +
        "    od_ts ts,\n" +
        "    row_op_ts\n" +
        "from dwd_trade_order_pre_process \n" +
        "where `type` = 'insert'"
    );

tableEnv
    .createTemporaryView("filtered_table", filteredTable);

// 创建kafka下单明细表
tableEnv
    .executeSql(
        "create table dwd_trade_order_detail (\n" +
        "    id string,\n" +
        "    order_id string,\n" +
        "    user_id string,\n" +
        "    sku_id string,\n" +
        "    sku_name string,\n" +
        "    province_id string,\n" +
        "    activity_id string,\n" +
        "    activity_rule_id string,\n" +
        "    coupon_id string,\n" +
        "    date_id string,\n" +
        "    create_time string,\n" +
        "    source_id string,\n" +
        "    source_type_code string,\n" +
        "    source_type_name string,\n" +
        "    sku_num string,\n" +
        "    split_original_amount string,\n" +
        "    split_activity_amount string,\n" +
        "    split_total_amount string,\n" +
        "    ts string,\n" +
        "    row_op_ts timestamp_ltz(3),\n" +
        "    primary key(id) not enforced\n" +
        ") with (\n" +
        "    'connector' = 'upsert-kafka',\n" +
        "    'topic' = 'dwd_trade_order_detail',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'key.format' = 'json',\n" +
        "    'value.format' = 'json'\n" +
        ")"
    );

// 将数据写到kafka
tableEnv.executeSql(
    "insert into dwd_trade_order_detail select * from filtered_table"
);
