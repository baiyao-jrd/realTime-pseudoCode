DwdTradeOrderCancelDetail

-> 交易域取消订单事实表

// 环境准备
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 并行度设置为4
env.setParallelism(4);
// 表环境
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

env.enableCheckpointing(3000L, CheckpointMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
env.enableExteranlizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.seconds(3)));
env.setStateBackend(new HashMapStateBackend());
env.getCheckpointConfig().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");

// 从kafka的dwd_trade_order_pre_process主题中读取数据并创建动态表
tableEnv
    .executeSql(
        "create table dwd_trade_order_pre_process (\n" +
        "    id String,\n" +
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
        "    source_tupe_name string,\n" +
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
        "    'commector' = 'kafka',\n" +
        "    'topic' = 'dwd_trade_order_pre_process',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'properties.group.id' = 'dwd_trade_cancel_detail_group',\n" +
        "    'scan.startup.mode' = 'group-offsets',\n" +
        "    'format' = 'json'\n" +
        ")"
    );

// 过滤取消订单明细数据
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
        "    operate_date_id date_id,\n" +
        "    operate_time cancel_time,\n" +
        "    source_id,\n" +
        "    source_type source_type_code,\n" +
        "    source_type_name,\n" +
        "    sku_num,\n" +
        "    split_original_amount,\n" +
        "    split_activity_amount,\n" +
        "    split_coupon_amount,\n" +
        "    split_total_amount,\n" +
        "    oi_ts ts,\n" +
        "    row_op_ts\n" +
        "from dwd_trade_order_pre_process\n" +
        "where `type` = 'update' and `old`['order_status'] is not null\n" +
        "and order_status = '1003'"
    );

tableEnv
    .createTemporaryView("filtered_table", filteredTable);

// 创建dwd_trade_order_cancel_detail表
tableEnv
    .executeSql(
        "create table dwd_trade_order_cancel_detail (\n" +
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
        "    cancel_time string,\n" +
        "    source_id string,\n" +
        "    source_type_code string,\n" +
        "    source_type_name string,\n" +
        "    sku_num string,\n" +
        "    split_original_amount string,\n" +
        "    split_activity_amount string,\n" +
        "    split_coupon_amount string,\n" +
        "    split_total_amount string,\n" +
        "    ts string,\n" +
        "    row_op_ts timestamp_ltz(3),\n" +
        "    primary key(id) not enforced\n" +
        ") with (\n" +
        "    'connector' = 'upsert-kafka',\n" +
        "    'topic' = 'dwd_trade_order_cancel_detail',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'key.format' = 'json',\n" +
        "    'value.format' = 'json'\n" +
        ")"
    );

// 将数据写入kafka
tableEnv
    .executeSql(
        "insert into dwd_trade_order_cancel_detail select * from filter_table"
    );