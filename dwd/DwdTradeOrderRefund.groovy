DwdTradeOrderRefund

-> 交易域退单事实表

// 流环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 并行度为4
env.setParallelism(4);
// 表环境
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
// 设置状态保留时间
tableEnv.getconfig().setIdleStateRetention(Duration.ofSeconds(10));

env.enableCheckpointing(3000L, CheckpointMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
env.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION));
env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.seconds(3)))
env.setStateBackend(new HashMapStateBackend());
env.getCheckpointConfig().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");
System.setProperty("HADOOP_USER_NAME", baiyao);

// 从kafka的topic_db中读取数据并创建动态表
tableEnv
    .sqlExecute(
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
        "    'properties.group.id' = 'dwd_trade_order_refund_group',\n" +
        "    'scan.startup.mode' = 'group-offsets',\n" +
        "    'format' = 'json'\n" +
        ")"
    );

// 从topic_db表中过滤出退单数据
Table orderRefundInfo = tableEnv
    .sqlQuery(
        "select\n" +
        "    data['id'] id,\n" +
        "    data['user_id'] user_id,\n" +
        "    data['order_id'] order_id,\n" +
        "    data['sku_id'] sku_id,\n" +
        "    data['refund_type'] refund_type,\n" +
        "    data['refund_num'] refund_num,\n" +
        "    data['refund_amount'] refund_amount,\n" +
        "    data['refund_reason_type'] refund_reason_type,\n" +
        "    data['refund_reason_txt'] refund_reason_txt,\n" +
        "    data['create_time'] create_time,\n" +
        "    proc_time,\n" +
        "    ts\n" +
        "from `topic_db`\n" +
        "where `table` = 'order_refund_info' and `type` = 'insert'"
    );

tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

// 读取订单表数据, 筛选退单数据
Table orderInfo = tableEnv
    .sqlQuery(
        "select\n" +
        "    data['id'] id,\n" +
        "    data['province_id'] province_id,\n" +
        "    `old`\n" +
        "from topic_db\n" +
        "where `table` = 'order_info' and `type` = 'update'\n" +
        "and data['order_status'] = '1005' and `old`['order_status'] is not null"
    );

tableEnv
    .createTemporaryView("order_info", orderInfo);

// 使用lookup方式从mysql读取base_dic字典表并创建动态表
tableEnv
    .executeSql(
        "create table base_dic (\n" +
        "    dic_code string,\n" +
        "    dic_name string,\n" +
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

// 关联3张表获得退单宽表
Table resultTable = tableEnv
    .sqlQuery(
        "select  \n" +
        "    ri.id,\n" +
        "    ri.user_id,\n" +
        "    ri.order_id,\n" +
        "    ri.sku_id,\n" +
        "\n" +
        "    oi.province_id,\n" +
        "    \n" +
        "    date_format(ri.create_time, 'yyyy-MM-dd') date_id,\n" +
        "    ri.create_time,\n" +
        "    ri.refund_type,\n" +
        "    type_dic.dic_name,\n" +
        "    ri.refund_reason_type,\n" +
        "    \n" +
        "    reason_dic.dic_name,\n" +
        "\n" +
        "    ri.refund_reason_txt,\n" +
        "    ri.refund_num,\n" +
        "    ri.refund_amount,\n" +
        "    ri.ts,\n" +
        "    current_row_timestamp() row_op_ts\n" +
        "from order_refund_info ri\n" +
        "join order_info oi on ri.order_id = oi.id\n" +
        "join base_dic for system_time as of ri.proc_time as reason_dic\n" +
        "on ri.refund_reason_type = reason_dic.dic_code"
    );

tableEnv.createTemporaryView("result_table", resultTable);

// 建立upsert-kafka dwd_trade_order_refund表
tableEnv
    .executeSql(
        "create table dwd_trade_order_refund (\n" +
        "    id,\n" +
        "    user_id,\n" +
        "    order_id,\n" +
        "    sku_id,\n" +
        "\n" +
        "    province_id,\n" +
        "    \n" +
        "    date_id,\n" +
        "    create_time,\n" +
        "    refund_type_code,\n" +
        "    refund_type_name,\n" +
        "    refund_reason_type_code,\n" +
        "    \n" +
        "    refund_reason_type_name,\n" +
        "\n" +
        "    refund_reason_txt,\n" +
        "    refund_num,\n" +
        "    refund_amount,\n" +
        "    ts,\n" +
        "    row_op_ts timestamp_ltz(3),\n" +
        "    primary key(id) not enforced\n" +
        ") with (\n" +
        "    'connector' = 'upsert-kafka',\n" +
        "    'topic' = 'dwd_trade_order_refund',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'key.format' = 'json',\n" +
        "    'value.format' = 'json'\n" +
        ")"
    );

// 写入kafka
tableEnv
    .executeSql(
        "insert into dwd_trade_order_refund select * from result_table"
    );