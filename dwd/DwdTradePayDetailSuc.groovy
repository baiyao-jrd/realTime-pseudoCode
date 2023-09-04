DwdTradePayDetailSuc

-> 交易域支付成功事实表

// 流处理环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 并行度为4
env.setParallelism(4);
// 表处理环境
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

env.enableCheckpointing(3000L, CheckpointMode.EXECTLY_ONCE);
env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
env.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.seconds(3)));
env.setStateBackend(new HashMapStateBackend());
env.getCheckpointConfig().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");
System.setProperty("HADOOP_USER_NAME", baiyao);

// 从dwd_trade_order_detail主题中读取数据并创建动态表
tableEnv
    .executeSql(
        "create table dwd_trade_order_detail (\n" +
        "    id string,\n" +
        "    order_id string,\n" +
        "    user_id string,\n" +
        "    sku_id string,\n" +
        "    sku_name string,\n" +
        "    province_id string,\n" +
        "    actvity_id string,\n" +
        "    activity_rule_id string,\n" +
        "    coupon_id string,\n" +
        "    date_id string,\n" +
        "    create_time string,\n" +
        "    source_id string,\n" +
        "    source_type_code string,\n" +
        "    source_type_name string,\n" +
        "    sku_name string,\n" +
        "    split_original_amount string,\n" +
        "    split_activity_amount string,\n" +
        "    split_coupon_amount string,\n" +
        "    split_total_amount string,\n" +
        "    ts string,\n" +
        "    row_op_ts timestamp_ltz(3)\n" +
        ") with (\n" +
        "    'connector' = 'kafka',\n" +
        "    'topic' = 'dwd_trade_order_detail',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'properties.group.id' = 'dwd_trade_pay_detail_suc',\n" +
        "    'scan.startup.mode' = 'group-offsets',\n" +
        "    'format' = 'json'\n" +
        ")"
    );

// 从topic_db主题中读取业务数据并创建动态表
tableEnv
    .executeSql(
        "create table topic_db (\n" +
        "    `database` string,\n" +
        "    `table` string,\n" +
        "    `type` string,\n" +
        "    `ts` string,\n" +
        "    `old` map<string, string>,\n" +
        "    `data` map<string, string>,\n" +
        "    proc_time as proctime()\n" +
        ") with (\n" +
        "    'connector' = 'kafka',\n" +
        "    'topic' = 'topic_db',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'properties.group.id' = 'dwd_trade_pay_detail_suc',\n" +
        "    'scan.startup.mode' = 'group-offsets',\n" +
        "    'format' = 'json'\n" +
        ")"
    );

// 筛选支付成功数据
Table paymentInfo = tableEnv
    .sqlQuery(
        "select\n" +
        "    data['user_id'] user_id,\n" +
        "    data['order_id'] order_id,\n" +
        "    data['payment_type'] payment_type,\n" +
        "    data['callback_time'] callback_time,\n" +
        "    `proc_time`,\n" +
        "    ts\n" +
        "from topic_db\n" +
        "where `table` = 'payment_info' and `type` = 'update' and data['payment_status'] = '1602'"
    );

tableEnv
    .createTemporaryView("payment_info", paymentInfo);

// 使用lookup方式从mysql中读取字典表数据并创建动态表
tableEnv
    .executeSql(
        "create table base_dic (\n" +
        "    dic_code string,\n" +
        "    dic_name string,\n" +
        "    primary key(dic_code) not enforced\n" +
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

// 关联三张表获得支付成功宽表
Table resultTable = tableEnv
    .sqlQuery(
        "select\n" +
        "    od.id order_detail_id,\n" +
        "    od.order_id,\n" +
        "    od.user_id,\n" +
        "    od.sku_id,\n" +
        "    od.sku_name,\n" +
        "    od.province_id,\n" +
        "    od.province_rule_id,\n" +
        "    od.coupon_id,\n" +
        "\n" +
        "    pi.payment_type payment_type_code,\n" +
        "    \n" +
        "    dic.dic_name payment_type_name,\n" +
        "\n" +
        "    pi.callback_time,\n" +
        "\n" +
        "    od.source_id,\n" +
        "    od.source_type_code,\n" +
        "    od.source_type_name,\n" +
        "    od.sku_num,\n" +
        "    od.split_original_amount,\n" +
        "    od.split_activity_amount,\n" +
        "    od.split_coupon_amount,\n" +
        "    od.split_total_amount split_payment_amount,\n" +
        "    pi.ts,\n" +
        "    od.row_op_ts row_op_ts\n" +
        "from dwd_trade_order_detail od\n" +
        "join payment_info pi on pi.order_id = od.order_id\n" +
        "join `base_dic` for system_time as of pi.proc_time as dic on pi.payment_type = dic.dic_code"
    )

tableEnv
    .createTemporaryView("result_table", resultTable);

// 创建dwd_trade_pay_detail动态表
tableEnv
    .executeSql(
        "create table dwd_trade_pay_detail_suc (\n" +
        "    order_detail_id string,\n" +
        "    order_id,\n" +
        "    user_id,\n" +
        "    sku_id,\n" +
        "    sku_name,\n" +
        "    province_id,\n" +
        "    activity_id,\n" +
        "    activity_rule_id,\n" +
        "    coupon_id,\n" +
        "\n" +
        "    payment_type_code,\n" +
        "    \n" +
        "    payment_type_name,\n" +
        "\n" +
        "    callback_time,\n" +
        "\n" +
        "    source_id,\n" +
        "    source_type_code,\n" +
        "    source_type_name,\n" +
        "    sku_num,\n" +
        "    split_original_amount,\n" +
        "    split_activity_amount,\n" +
        "    split_coupon_amount,\n" +
        "    split_payment_amount,\n" +
        "    ts,\n" +
        "    row_op_ts timestamp_ltz(3)\n" +
        "    primary key(order_detail_id) not enforced\n" +
        ") with (\n" +
        "    'connector' = 'upsert-kafka',\n" +
        "    'topic' = 'dwd_trade_pay_detail_suc',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'key.format' = 'json',\n" +
        "    'value.format' = 'json'\n" +
        ")"
    );

// 将数据写入kafka中
tableEnv
    .executeSql(
        "insert into dwd_trade_pay_detail_suc select * from result_table"
    );