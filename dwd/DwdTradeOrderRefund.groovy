DwdTradeOrderRefund

-> 交易域退单事务事实表
// 总体思路: 
// 从kafka的topic_db主题中读取业务数据, 然后筛选退单表order_refund_info的数据, 同时筛选
// 满足条件的订单表order_info的数据, 建立mysql-lookup字典表, 关联三张表获得退单明细宽表



// 流环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 并行度为4
env.setParallelism(4);
// 表环境
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// 设置状态保留时间
// 用户执行一次退单操作时，order_refund_info 会插入多条数据，同时 order_info 表的 order_status 数据会发生修改，两张表不存在业务上的时间滞后问题，因此仅考虑可能的乱序即可，ttl 设置为 5s。
tableEnv.getconfig().setIdleStateRetention(Duration.ofSeconds(5));

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
// 退单业务过程最细粒度的操作: 表示每个订单中的每个sku的退单操作, 因此将order_refund_info作为主表
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

// 筛选订单表数据, 获取province_id维度

// 退单操作发生时，订单表的 order_status 字段值会由1002（已支付）更新为 1005（退款中）。
// 订单表中的数据要满足三个条件：
    1. order_status 为 1005 -> 退款中
    2. 操作类型为 update
    3. 更新的字段为 order_status, 该字段发生变化时, 变更数据中 old 字段下 order_status 的值不为 null -> 为1002
// 这一步是否对订单表数据筛选并不影响查询结果，提前对数据进行过滤是为了减少数据量，减少性能消耗。
// 退单表order_info
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
// 获取退款类型名称以及退款原因类型名称
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

// 关联3张表获得退单明细宽表
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
        "join base_dic for system_time as of ri.proc_time as type_dic\n" +
        "on ri.refund_type = type_dic.dic_code" +
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

// 写入kafka退单明细主题
tableEnv
    .executeSql(
        "insert into dwd_trade_order_refund select * from result_table"
    );