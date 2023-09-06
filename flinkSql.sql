create table topic_db (
    `database` String,
    `table` String,
    `type` String,
    `ts` String,
    `old` MAP<String, String>,
    `data` MAP<String, String>,
    proc_time as proctime() 
) with (
    'connector' = 'kafka',
    'topic' = 'topic_db',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'properties.group.id' = 'dwd_trade_cart_add_group',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json'
);


select 
    data['id'] id,
    data['user_id'] user_id,
    data['sku_id'] sku_id,
    data['source_type'] source_type,
    ts,
    proc_time,
    if (
        `type` = 'insert', data['sku_num'], CAST(
            (cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string
        )
    ) sku_num 
from topic_db
where `table` = `cart_info` and
      (
        `type` = 'insert' or 
        (
            `type` = 'update' and `old`['sku_num'] is not null and CAST(data['sku_num'] as int) > CAST(`old`['sku_num'] as int)
        )
      );

create table base_dic (
    dic_code String,
    dic_name String,
    primary key (dic_code) not enforced
) with (
    'connector' = 'jdbc',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'url' = 'jdbc:mysql://zoo1:3306/gmall',
    'table-name' = 'base_dic',
    'lookup.cache.max-rows' = '200',
    'lookup.cache.ttl' = '1 hour',
    'username' = 'root',
    'password' = '123456'
);
    
select
    cartTable.id,
    cartTable.user_id,
    carttable.sku_id,
    cartTable.sku_num,
    cartTable.source_type,
    dic.dic_name,
    cartTable.ts
from cart_table as cartTable
join base_dic for system_time as of cartTable.proc_time as dic
on cartTable.source_type = dic.dic_code;

create table dwd_trade_cart_add (
    id String,
    user_id String,
    sku_id String,
    sku_num String,
    source_type_code String,
    source_type_name String,
    ts String,
    primary key (id) not enforced
) with (
    'connector' = 'upsert-kafka',
    'topic' = 'dwd_trade_cart_add',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

create table topic_db (
    `database` String,
    `table` String,
    `type` String,
    `ts` String,
    `old` MAP<String, String>,
    `data` MAP<String, String>,
    proc_time as proctime()
) with (
    'connector' = 'kafka',
    'topic' = 'topic_db',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'properties.group.id' = 'dwd_trade_order_pre_process_group',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json'
);

select
    data['id'] id,
    data['order_id'] order_id,
    data['sku_id'] sku_id,
    data['sku_name'] sku_name,
    data['create_time'] create_time,
    data['source_id'] source_id,
    data['source_type'] source_type,
    data['sku_num'] sku_num,
    cast(cast(data['sku_num'] as decimal(16, 2)) * cast(data['order_price'] as decimal(16, 2)) as String) split_original_amount,
    data['split_total_amount'] split_total_amount,
    data['split_activity_amount'] split_activity_amount,
    data['split_coupon_amount'] split_coupon_amount,
    ts od_ts,
    proc_time
from `topic_db` where `table` = 'order_detail' and `type` = 'insert';


select
    data['id'] id,
    data['user_id'] user_id,
    data['province_id'] province_id,
    data['operate_time'] operate_time,
    data['order_status'] order_status,
    `type`,
    `old`,
    ts oi_ts
from `topic_db`
where `table` = 'order_info' and (`type` = 'insert' or `type` = 'update');


select
    data['order_detail_id'] order_detail_id,
    data['activity_id'] activity_id,
    data['activity_rule_id'] activity_rule_id
from `topic_db`
where `table` = 'order_detail_activity'
and `type` = 'insert';


select
    data['order_detail_id'] order_detail_id,
    data['coupon_id'] coupon_id
from `topic_db`
where `table` = 'order_detail_coupon' and type = 'insert';


create table base_dic (
    dic_code String,
    dic_name String,
    primary key (dic_code) not enforced
) with (
    'connector' = 'jdbc',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'url' = 'jdbc:mysql://zoo1:3306/gmall',
    'table-name' = 'base_dic',
    'lookup.cache.max-rows' = '200',
    'lookup.cache.ttl' = '1 hour',
    'username' = 'root',
    'password' = '123456'
);

select
    od.id,
    od.order_id,
    od.sku_id,
    od.sku_name,
    date_format(od.create_time, 'yyyy-MM-dd') date_id,
    od.create_time,
    od.source_id,
    od.source_type,
    od.sku_num,
    od.split_original_amount,
    od.split_activity_amount,
    od.split_coupon_amount,
    od.split_total_amount,
    od.od_ts,

    oi.user_id,
    oi.province_id,
    date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id,
    oi.operate_time,
    oi.order_status,
    oi.`type`,
    oi.`old`,
    oi.oi_ts,

    act.activity_id,
    act.activity_rule_id,

    cou.coupon_id,

    dic.dic_name source_type_name,

    current_row_timestamp() row_op_ts
from order_detail od
join order_info oi on od.order_id = oi.id
left join order_detail_activity act on od.id = act.order_detail_id
left join order_detail_coupon cou on od.id = cou.order_detail_id
join `base_dic` for system_time as of od.proc_time as dic
on od.source_type = dic.dic_code;


create table dwd_trade_order_pre_process (
    id string,
    order_id string,
    sku_id string,
    sku_name string,
    date_id string,
    create_time string,
    source_id string,
    source_type string,
    sku_num string,
    split_original_amount string,
    split_activity_amount string,
    split_coupon_amount string,
    split_total_amount string,
    od_ts string,

    user_id string,
    province_id string,
    operate_date_id string,
    operate_time string,
    order_status string,
    `type` string,
    `old` map<string, string>,
    oi_ts string,

    activity_id string,
    activity_rule_id string,

    coupon_id string,

    source_type_name string,

    row_op_ts timestamp_ltz(3),
    primary key(id) not enforced
) with (
    'connector' = 'upsert-kafka',
    'topic' = 'dwd_trade_order_pre_process',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);


create table dwd_trade_order_pre_process (
    id string,
    order_id string,
    user_id string,
    order_status string,
    sku_id string,
    sku_name string,
    province_id string,
    activity_id string,
    activity_rule_id string,
    coupon_id string,
    date_id string,
    create_time string,
    operate_date_id string,
    operate_time string,
    source_id string,
    source_type string,
    source_type_name string,
    sku_num string,
    split_original_amount string,
    split_activity_amount string,
    split_coupon_amount string,
    split_total_amount string,
    `type` string,
    `old` map<string, string>,
    od_ts string,
    oi_ts string,
    row_op_ts timestamp_ltz(3)
) with (
    'connector' = 'kafka',
    'topic' = 'dwd_trade_order_pre_process',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'properties.group.id' = 'dwd_trade_order_detail_group',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json'
);


select
    id,
    order_id,
    user_id,
    sku_id,
    sku_name,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_id,
    create_time,
    source_id,
    source_type source_type_code,
    source_type_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount,
    od_ts ts,
    row_op_ts
from dwd_trade_order_pre_process 
where `type` = 'insert';


create table dwd_trade_order_detail (
    id string,
    order_id string,
    user_id string,
    sku_id string,
    sku_name string,
    province_id string,
    activity_id string,
    activity_rule_id string,
    coupon_id string,
    date_id string,
    create_time string,
    source_id string,
    source_type_code string,
    source_type_name string,
    sku_num string,
    split_original_amount string,
    split_activity_amount string,
    split_total_amount string,
    ts string,
    row_op_ts timestamp_ltz(3),
    primary key(id) not enforced
) with (
    'connector' = 'upsert-kafka',
    'topic' = 'dwd_trade_order_detail',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

create table dwd_trade_order_pre_process (
    id String,
    order_id string,
    user_id string,
    order_status string,
    sku_id string,
    sku_name string,
    province_id string,
    activity_id string,
    activity_rule_id string,
    coupon_id string,
    date_id string,
    create_time string,
    operate_date_id string,
    operate_time string,
    source_id string,
    source_type string,
    source_tupe_name string,
    sku_num string,
    split_original_amount string,
    split_activity_amount string,
    split_coupon_amount string,
    split_total_amount string,
    `type` string,
    `old` map<string, string>,
    od_ts string,
    oi_ts string,
    row_op_ts timestamp_ltz(3)
) with (
    'commector' = 'kafka',
    'topic' = 'dwd_trade_order_pre_process',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'properties.group.id' = 'dwd_trade_cancel_detail_group',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json'
);

select
    id,
    order_id,
    user_id,
    sku_id,
    sku_name,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    operate_date_id date_id,
    operate_time cancel_time,
    source_id,
    source_type source_type_code,
    source_type_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_total_amount,
    oi_ts ts,
    row_op_ts
from dwd_trade_order_pre_process
where `type` = 'update' and `old`['order_status'] is not null
and order_status = '1003';


create table dwd_trade_order_cancel_detail (
    id string,
    order_id string,
    user_id string,
    sku_id string,
    sku_name string,
    province_id string,
    activity_id string,
    activity_rule_id string,
    coupon_id string,
    date_id string,
    cancel_time string,
    source_id string,
    source_type_code string,
    source_type_name string,
    sku_num string,
    split_original_amount string,
    split_activity_amount string,
    split_coupon_amount string,
    split_total_amount string,
    ts string,
    row_op_ts timestamp_ltz(3),
    primary key(id) not enforced
) with (
    'connector' = 'upsert-kafka',
    'topic' = 'dwd_trade_order_cancel_detail',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

create table dwd_trade_order_detail (
    id string,
    order_id string,
    user_id string,
    sku_id string,
    sku_name string,
    province_id string,
    actvity_id string,
    activity_rule_id string,
    coupon_id string,
    date_id string,
    create_time string,
    source_id string,
    source_type_code string,
    source_type_name string,
    sku_name string,
    split_original_amount string,
    split_activity_amount string,
    split_coupon_amount string,
    split_total_amount string,
    ts string,
    row_op_ts timestamp_ltz(3)
) with (
    'connector' = 'kafka',
    'topic' = 'dwd_trade_order_detail',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'properties.group.id' = 'dwd_trade_pay_detail_suc',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json'
);

create table topic_db (
    `database` string,
    `table` string,
    `type` string,
    `ts` string,
    `old` map<string, string>,
    `data` map<string, string>,
    proc_time as proctime()
) with (
    'connector' = 'kafka',
    'topic' = 'topic_db',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'properties.group.id' = 'dwd_trade_pay_detail_suc',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json'
);

select
    data['user_id'] user_id,
    data['order_id'] order_id,
    data['payment_type'] payment_type,
    data['callback_time'] callback_time,
    `proc_time`,
    ts
from topic_db
where `table` = 'payment_info' and `type` = 'update' and data['payment_status'] = '1602';


create table base_dic (
    dic_code string,
    dic_name string,
    primary key(dic_code) not enforced
) with (
    'connector' = 'jdbc',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'url' = 'jdbc:mysql://zoo1:3306/gmall',
    'table-name' = 'base_dic',
    'lookup.cache.max-rows' = '200',
    'lookup.cache.ttl' = '1 hour',
    'username' = 'root',
    'password' = '123456'
);

select
    od.id order_detail_id,
    od.order_id,
    od.user_id,
    od.sku_id,
    od.sku_name,
    od.province_id,
    od.province_rule_id,
    od.coupon_id,

    pi.payment_type payment_type_code,
    
    dic.dic_name payment_type_name,

    pi.callback_time,

    od.source_id,
    od.source_type_code,
    od.source_type_name,
    od.sku_num,
    od.split_original_amount,
    od.split_activity_amount,
    od.split_coupon_amount,
    od.split_total_amount split_payment_amount,
    pi.ts,
    od.row_op_ts row_op_ts
from dwd_trade_order_detail od
join payment_info pi on pi.order_id = od.order_id
join `base_dic` for system_time as of pi.proc_time as dic on pi.payment_type = dic.dic_code;


create table dwd_trade_pay_detail_suc (
    order_detail_id string,
    order_id,
    user_id,
    sku_id,
    sku_name,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,

    payment_type_code,
    
    payment_type_name,

    callback_time,

    source_id,
    source_type_code,
    source_type_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_payment_amount,
    ts,
    row_op_ts timestamp_ltz(3)
    primary key(order_detail_id) not enforced
) with (
    'connector' = 'upsert-kafka',
    'topic' = 'dwd_trade_pay_detail_suc',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

create table topic_db (
    `database` string,
    `table` string,
    `type` string,
    `ts` string,
    `old` map<string, string>,
    `data` map<string, string>,
    proc_time as proctime()
) with (
    'connector' = 'kafka',
    'topic' = 'topic_db',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'properties.group.id' = 'dwd_trade_order_refund_group',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json'
);


select
    data['id'] id,
    data['user_id'] user_id,
    data['order_id'] order_id,
    data['sku_id'] sku_id,
    data['refund_type'] refund_type,
    data['refund_num'] refund_num,
    data['refund_amount'] refund_amount,
    data['refund_reason_type'] refund_reason_type,
    data['refund_reason_txt'] refund_reason_txt,
    data['create_time'] create_time,
    proc_time,
    ts
from `topic_db`
where `table` = 'order_refund_info' and `type` = 'insert';


select
    data['id'] id,
    data['province_id'] province_id,
    `old`
from topic_db
where `table` = 'order_info' and `type` = 'update'
and data['order_status'] = '1005' and `old`['order_status'] is not null;

create table base_dic (
    dic_code string,
    dic_name string,
    primary key (dic_code) not enforced
) with (
    'connector' = 'jdbc',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'url' = 'jdbc:mysql://zoo1:3306/gmall',
    'table-name' = 'base_dic',
    'lookup.cache.max-rows' = '200',
    'lookup.cache.ttl' = '1 hour',
    'username' = 'root',
    'password' = '123456'
);

select  
    ri.id,
    ri.user_id,
    ri.order_id,
    ri.sku_id,

    oi.province_id,
    
    date_format(ri.create_time, 'yyyy-MM-dd') date_id,
    ri.create_time,
    ri.refund_type,
    type_dic.dic_name,
    ri.refund_reason_type,
    
    reason_dic.dic_name,

    ri.refund_reason_txt,
    ri.refund_num,
    ri.refund_amount,
    ri.ts,
    current_row_timestamp() row_op_ts
from order_refund_info ri
join order_info oi on ri.order_id = oi.id
join base_dic for system_time as of ri.proc_time as reason_dic
on ri.refund_reason_type = reason_dic.dic_code;


create table dwd_trade_order_refund (
    id,
    user_id,
    order_id,
    sku_id,

    province_id,
    
    date_id,
    create_time,
    refund_type_code,
    refund_type_name,
    refund_reason_type_code,
    
    refund_reason_type_name,

    refund_reason_txt,
    refund_num,
    refund_amount,
    ts,
    row_op_ts timestamp_ltz(3),
    primary key(id) not enforced
) with (
    'connector' = 'upsert-kafka',
    'topic' = 'dwd_trade_order_refund',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

create table topic_db (
    `database` string,
    `table` string,
    `ts` string,
    `type` string,
    `old` map<string, string>,
    `data` map<string, string>,
    proc_time as proctime()
) with (
    'connector' = 'kafka',
    'topic' = 'topic_db',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'properties.group.id' = 'dwd_trade_refund_pay_success_group',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json'
);

create table base_dic (
    dic_code string,
    dic_name string,
    primary key(dic_code) not enforced
) with (
    'connector' = 'jdbc',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'url' = 'jdbc:mysql://zoo1:3306/gmall',
    'table-name' = 'base_dic',
    'lookup.cache.max-rows' = '200',
    'lookup.cache.ttl' = '1 hour',
    'username' = 'root',
    'password' = '123456'
);

select
    data['id'] id,
    data['order_id'] order_id,
    data['sku_id'] sku_id,
    data['payment_type'] payment_type,
    data['callback_time'] callback_time,
    data['total_amount'] total_amount,
    proc_time,
    ts
from `topic_db`
where `table` = 'refund_payment' and `type` = 'update'
and data['refund_status'] = '0701' and `old`['refund_status'] is not null;

select
    data['id'] id,
    data['user_id'] user_id,
    data['province_id'] province_id,
    `old`
from topic_db
where `table` = order_info and `type` = 'update'
and data['order_status'] = '1006' and `old`['order_status'] is not null;

select
    data['order_id'] order_id,
    data['sku_id'] sku_id,
    data['refund_num'] refund_num,
    `old`
from topic_db
where `table` = 'order_refund_info' and `type` = 'update'
and data['refund_status'] = '0705' and `old`['refund_status'] is not null;

select
    rp.id,
    oi.user_id,
    rp.order_id,
    rp.sku_id,
    oi.province_id,
    rp.payment_type,
    dic.dic_name payment_type_name,
    date_format(rp.callback_time, 'yyyy-MM-dd') date_id,
    rp.callback_time,
    ri.refund_num,
    rp.total_amount,
    rp.ts,
    current_row_timestamp() row_op_ts
from refund_payment rp
join order_info oi on rp.order_id = oi.id
join order_refund_info ri on rp.order_id = ri.order_id and rp.sku_id = ri.sku_id
join base_dic for system_time as of rp.proc_time as dic
on rp.payment_type = dic.dic_code;

create table dwd_trade_refund_pay_suc (
    id string,
    user_id string,
    order_id string,
    sku_id string,
    province_id string,
    payment_type_code string,
    payment_type_name string,
    date_id string,
    callback_time string,
    refund_num string,
    total_amount string,
    ts string,
    row_op_ts timestamp_ltz(3),
    primary key(id) not enforced
) with (
    'connector' = 'upsert-kafka',
    'topic' = 'dwd_trade_refund_pay_suc',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

create table page_log (
    common map<string, string>,
    page map<string, string>,
    ts bigint,
    rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),
    watermark for rowtime as rowtime - interval '3' second
) with (
    'connector' = 'kafka',
    'topic' = 'dwd_traffic_page_log',
    'properties.bootstrap.servers' = 'zoo1:9092',
    'properties.group.id' = 'dws_traffic_keyword_group',
    'scan.startup.mode' = 'group-offsets',
    'format' = 'json'
);

select
    page['item'] fullword,
    rowtime
from page_log
where page['last_page_id'] = 'search' and page['item_type'] = 'keyword'
and page['item'] is not null;


select
    keyword,
    rowtime
from search_table, lateral table(ik_analyze(fullword) t(keyword));

select
    date_format(tumble_start(row_time, interval '10' second), 'yyyy-MM-dd HH:mm:ss') stt,
    date_format(tumble_end(row_time, interval '10' second), 'yyyy-MM-dd HH:mm:ss') edt,
    search source,
    keyword,
    count(*) keyword_count,
    unix_timestamp() * 1000 ts
from split_table
group by tumble(rowtime, interval '10' second), keyword;

drop table if exists dws_traffic_source_keyword_page_view_window;

create table if not exists dws_traffic_source_keyword_page_view_window (
    stt DateTime,
    edt DateTime,
    source String,
    keyword String,
    keyword_count UInt64,
    ts UInt64
) engine = ReplicatedReplacingMergeTree(ts)
  partition by toYYYYMMDD(stt)
  order by (stt, edt, source, keyword);  