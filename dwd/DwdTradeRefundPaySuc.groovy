DwdTradeRefundPaySuc

-> 交易域退款成功事实表

// 流环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 并行度
env.setParallelism(4);
// 表环境
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

env.enableCheckpointing(3000L, CheckpointMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
env.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.seconds(3)));
env.setStateBackend(new HashMapStateBackend());
env.getCheckpointConfig().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");
System.setProperty("HADOOP_USER_NAME", baiyao);

// 从kafka的topic_db读取业务数据并创建动态表
// topic_db
// {
//     "database": "gmall_realtime",
//     "table": "payment_info",
//     "type": "update",
//     "ts": 1662027265,
//     "xid": 10255,
//     "xoffset": 1504,
//     "data": {
//         "id": 2574,
//         "out_trade_no": "184355667412131",
//         "order_id": 4875,
//         "user_id": 63,
//         "payment_type": "1102",
//         "trade_no": "4323448238582877448738717915119727",
//         "total_amount": 62.00,
//         "subject": "口红 唇膏 赤茶等3件商品",
//         "payment_status": "1602",
//         "create_time": "2022-09-01 18:14:25",
//         "callback_time": "2022-09-01 18:14:45",
//         "callback_content": "NcmbuBPqAX"
//     },
//     "old": {
//         "payment_status": "1601",
//         "callback_time": null,
//         "callback_content": null
//     }
// }

tableEnv
    .executeSql(
        "create table topic_db (\n" +
        "    `database` string,\n" +
        "    `table` string,\n" +
        "    `ts` string,\n" +
        "    `type` string,\n" +
        "    `old` map<string, string>,\n" +
        "    `data` map<string, string>,\n" +
        "    proc_time as proctime()\n" +
        ") with (\n" +
        "    'connector' = 'kafka',\n" +
        "    'topic' = 'topic_db',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'properties.group.id' = 'dwd_trade_refund_pay_success_group',\n" +
        "    'scan.startup.mode' = 'group-offsets',\n" +
        "    'format' = 'json'\n" +
        ")"
    );

// 连接MySql读取字典表base_dic数据并创建动态表
//  dic_code  dic_name
------------  ----------------
    10        单据状态                      
    1001      未支付                   
    1002      已支付                   
    1003      已取消                   
    1004      已完成                   
    1005      退款中                   
    1006      退款完成                                 
    07        退单状态                  
    0701      商家审核中                 
    0702      商家审核通过                
    0703      商家审核未通过               
    0704      买家已发货                 
    0705      退单完成                  
    0706      退单失败     


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
        ")";
    );

// 读取退款表数据, 筛选退款成功数据
// refund_payment
    id  out_trade_no     order_id  sku_id  payment_type  trade_no  total_amount  subject  refund_status  create_time          callback_time        callback_content  
------  ---------------  --------  ------  ------------  --------  ------------  -------  -------------  -------------------  -------------------  ------------------
     1  794332583599149      4865      19  1101          (NULL)    35997.00      退款       0701           2020-06-10 19:47:03  2020-06-10 19:47:03  (NULL)            
     2  844271555599764      4868      21  1101          (NULL)    9897.00       退款       0701           2020-06-10 19:47:03  2020-06-10 19:47:03  (NULL)            
     3  155876315913142      4869      27  1101          (NULL)    129.00        退款       0701           2020-06-11 19:49:37  2020-06-11 19:49:37  (NULL)            
     4  925666138456735      4872       4  1101          (NULL)    999.00        退款       0701           2020-06-11 19:49:37  2020-06-11 19:49:37  (NULL) 

Table refundPayment = tableEnv
    .sqlQuery(
        "select\n" +
        "    data['id'] id,\n" +
        "    data['order_id'] order_id,\n" +
        "    data['sku_id'] sku_id,\n" +
        "    data['payment_type'] payment_type,\n" +
        "    data['callback_time'] callback_time,\n" +
        "    data['total_amount'] total_amount,\n" +
        "    proc_time,\n" +
        "    ts\n" +
        "from `topic_db`\n" +
        "where `table` = 'refund_payment' and `type` = 'update'\n" +
        "and data['refund_status'] = '0701' and `old`['refund_status'] is not null"
    );

tableEnv.createTemporaryView("refund_payment", refundPayment);

// 读取订单表数据, 过滤退款成功订单数据
// 订单表状态变为'退款完成' -> 1006
// order_info

    id  consignee     consignee_tel  total_amount  order_status  user_id  payment_way  delivery_address                     order_comment  out_trade_no     trade_body                                                                                                                                                              create_time          operate_time         expire_time          process_status  tracking_no  parent_order_id  img_url                          province_id  activity_reduce_amount  coupon_reduce_amount  original_total_amount  feight_fee  feight_fee_reduce  refundable_time  
------  ------------  -------------  ------------  ------------  -------  -----------  -----------------------------------  -------------  ---------------  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------  -------------------  -------------------  -------------------  --------------  -----------  ---------------  -------------------------------  -----------  ----------------------  --------------------  ---------------------  ----------  -----------------  -----------------
  4863  于天达           13286713006    15490.00      1003               28  (NULL)       第16大街第36号楼2单元951门                    描述989578       882158966342598  CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M02干玫瑰等4件商品                                                                                                                              2020-06-10 19:47:00  2020-06-10 19:47:02  2020-06-10 20:02:00  (NULL)          (NULL)                (NULL)  http://img.gmall.com/573743.jpg           16  500.00                  30.00                 16002.00               18.00       (NULL)             (NULL)           
  4864  熊燕彩           13473139694    122.00        1006               88  (NULL)       第14大街第22号楼4单元226门                    描述621364       655816758361257  十月稻田 长粒香大米 东北大米 东北香米 5kg等3件商品                                                                                                                                           2020-06-10 19:47:00  2020-06-14 15:27:46  2020-06-10 20:02:00  (NULL)          (NULL)                (NULL)  http://img.gmall.com/592465.jpg           18  0.00                    0.00                  117.00                 5.00        (NULL)             (NULL)  

Table orderInfo = tableEnv
    .sqlQuery(
        "select\n" +
        "    data['id'] id,\n" +
        "    data['user_id'] user_id,\n" +
        "    data['province_id'] province_id,\n" +
        "    `old`\n" +
        "from topic_db\n" +
        "where `table` = order_info and `type` = 'update'\n" +
        "and data['order_status'] = '1006' and `old`['order_status'] is not null"
    );

tableEnv.createTemporaryView("order_info", orderInfo);

// 读取退单表数据并过滤退款成功数据
// order_refund_info
    id  user_id  order_id  sku_id  refund_type  refund_num  refund_amount  refund_reason_type  refund_reason_txt                refund_status  create_time          
------  -------  --------  ------  -----------  ----------  -------------  ------------------  -------------------------------  -------------  ---------------------
   748      105      4865      19  1502                  3  35997.00       1301                退款原因具体：8372986279                0705           2020-06-10 19:47:03  
   749      150      4868      21  1501                  3  9897.00        1302                退款原因具体：7661310557                0705           2020-06-10 19:47:03  
   750      165      4869      27  1502                  1  129.00         1303                退款原因具体：6988302556                0705           2020-06-11 19:49:37  
   751       38      4872       4  1502                  1  999.00         1301                退款原因具体：0397070140                0705           2020-06-11 19:49:37  

Table orderRefundInfo = tableEnv
    .sqlQuery(
        "select\n" +
        "    data['order_id'] order_id,\n" +
        "    data['sku_id'] sku_id,\n" +
        "    data['refund_num'] refund_num,\n" +
        "    `old`\n" +
        "from topic_db\n" +
        "where `table` = 'order_refund_info' and `type` = 'update'\n" +
        "and data['refund_status'] = '0705' and `old`['refund_status'] is not null"
    );

tableEnv.executeSql("order_refund_info", orderRefundInfo);

// 关联四张表获得退款成功表
Table resuleTable = tableEnv
    .sqlQuery(
        "select\n" +
        "    rp.id,\n" +
        "    oi.user_id,\n" +
        "    rp.order_id,\n" +
        "    rp.sku_id,\n" +
        "    oi.province_id,\n" +
        "    rp.payment_type,\n" +
        "    dic.dic_name payment_type_name,\n" +
        "    date_format(rp.callback_time, 'yyyy-MM-dd') date_id,\n" +
        "    rp.callback_time,\n" +
        "    ri.refund_num,\n" +
        "    rp.total_amount,\n" +
        "    rp.ts,\n" +
        "    current_row_timestamp() row_op_ts\n" +
        "from refund_payment rp\n" +
        "join order_info oi on rp.order_id = oi.id\n" +
        "join order_refund_info ri on rp.order_id = ri.order_id and rp.sku_id = ri.sku_id\n" +
        "join base_dic for system_time as of rp.proc_time as dic\n" +
        "on rp.payment_type = dic.dic_code"
    );

tableEnv.createTemporyView("result_table", resultTable);

// 创建upsert-kafka dwd_trade_refund_pay_suc动态表
tableEnv
    .executeSql(
        "create table dwd_trade_refund_pay_suc (\n" +
        "    id string,\n" +
        "    user_id string,\n" +
        "    order_id string,\n" +
        "    sku_id string,\n" +
        "    province_id string,\n" +
        "    payment_type_code string,\n" +
        "    payment_type_name string,\n" +
        "    date_id string,\n" +
        "    callback_time string,\n" +
        "    refund_num string,\n" +
        "    total_amount string,\n" +
        "    ts string,\n" +
        "    row_op_ts timestamp_ltz(3),\n" +
        "    primary key(id) not enforced\n" +
        ") with (\n" +
        "    'connector' = 'upsert-kafka',\n" +
        "    'topic' = 'dwd_trade_refund_pay_suc',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'key.format' = 'json',\n" +
        "    'value.format' = 'json'\n" +
        ")"
    );

// 写入kafka
tableEnv.executeSql(
    "insert into dwd_trade_refund_pay_suc select * from result_table"
)