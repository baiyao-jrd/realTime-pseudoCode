DwdTradeOrderPreProcess

-> 交易域订单预处理表
// 思路:
    经过分析，订单明细表和取消订单明细表的数据来源、表结构都相同，差别只在业务过程和过滤条件，为了减少重复计算，将两张表公共的关联过程提取出来，形成订单预处理表。

    关联订单明细表、订单表、订单明细活动关联表、订单明细优惠券关联表四张事实业务表和字典表（维度业务表）形成订单预处理表，写入 Kafka 对应主题。

    本节形成的预处理表中要保留ods层过来的订单表type 和 old 字段，用于过滤订单明细数据和取消订单明细数据。

// (1)设置 ttl；
// ttl（time-to-live）即存活时间。表之间做普通关联时，底层会将两张表的数据维护到状态中，默认情况下状态永远不会清空，这样会对内存造成极大的压力。表状态的 ttl 是 Idle（空闲，即状态未被更新）状态被保留的最短时间，假设 ttl 为 10s，若状态中的数据在 10s 内未被更新，则未来的某个时间会被清除（故而 ttl 是最短存活时间）。ttl 默认值为 0，表示永远不会清空状态。
// 下单操作发生时，订单明细表、订单表、订单明细优惠券关联表和订单明细活动关联表的数据操作类型均为insert，不存在业务上的滞后问题，只考虑可能的数据乱序即可；而取消订单时只有订单表的状态发生变化，要和其它三张表关联，就需要考虑取消订单的延迟，通常在支付前均可取消订单，因此将 ttl 设置为 15min + 5s。
// 要注意：前文提到，本项目保证了同一分区、同一并行度的数据有序。此处的乱序与之并不冲突，以下单业务过程为例，用户完成下单操作时，订单表中会插入一条数据，订单明细表中会插入与之对应的多条数据，本项目业务数据是按照primary_key主键分区进入 Kafka 的，虽然同分区数据有序，但是同一张业务表的数据可能进入多个分区，会乱序。这样一来，订单表数据与对应的订单明细数据可能被属于其它订单的数据“插队”，因而导致主表或从表数据迟到，可能 join 不上，为了应对这种情况，设置乱序程度，让状态中的数据等待一段时间。
// (2)从topic_db主题读取业务数据要调用 PROCTIME() 函数获取系统时间作为与字典表做 Lookup Join 的处理时间字段
// (3)筛选订单明细表数据: 
// 应尽可能保证事实表的粒度为最细粒度，在下单业务过程中，最细粒度的事件为一个订单的一个 SKU 的下单操作，订单明细表的粒度与最细粒度相同，将其作为主表
// (4)筛选订单表数据
// 通过该表获取 user_id 和 province_id。保留 type 字段和 old 字段用于过滤订单明细数据和取消订单明细数据
// (5)筛选订单明细活动关联表数据
// 通过该表获取活动 id 和活动规则 id
// (6)筛选订单明细优惠券关联表数据；
// 通过该表获取优惠券 id。
// (7)建立 MySQL-Lookup 字典表
// 通过字典表获取订单来源类型名称
// (8)关联上述五张表获得订单宽表，写入 Kafka 主题
// 事实表的粒度应为最细粒度，在下单和取消订单业务过程中，最细粒度为一个 sku 的下单或取消订单操作，与订单明细表粒度相同，将其作为主表。
// -> 订单明细表和订单表的所有记录在另一张表中都有对应数据，内连接即可。
// -> 订单明细数据未必参加了活动也未必使用了优惠券，因此要保留订单明细独有数据，所以与订单明细活动关联表和订单明细优惠券关联表的关联使用 left join。
// -> 与字典表的关联是为了获取 source_type 对应的 source_type_name，订单明细数据在字典表中一定有对应，内连接即可。

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