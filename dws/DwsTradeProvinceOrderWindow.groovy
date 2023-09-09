DwsTradeProvinceOrderWindow

-> 交易域省份粒度下单业务过程聚合统计
        统计各省份各窗口的订单数和订单金额

// 流环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 并行度
env.setParallelism(4);

// 检查点设置
env.enableCheckointing(5000L, checkpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
env.setRestartRestrategy(RestartStrtegies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
env.setSateBackend(new HashMapStateBackend());
env.getCheckpointConfig().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");
System.setProperty("HADOOP_USER_NAME", baiyao);

// 创建消费者对象消费dwd_trade_order_detail主题并封装为流
reduceDS = env
    .addSource(
        new FlinkKafkaConsumer<String>() {
            "dwd_trade_order_detail",
            new KafkaDeserializationSchema<String>() {
                isEndOfStream return false;

                String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
                    if (consumerRecord.value != null) {
                        return new String(consumerRecord.value)
                    }
                    return false;
                }

                TypeInformation<String> getProducedType() {
                    return TypeInformation.of(String.class)
                }
            },

            // Properties props = new Properties();
            // props.setProperty("bootstrap.servers", "zoo1:9092");
            // props.setProperty("group.id", "dws_trade_province_group");

            props
        }
    )
    // 将String类型数据转换为JSONObject类型
    .map(JSON::parseObject)
    // {
    //     "create_time": "2022-09-01 10:15:55",
    //     "sku_num": "1",
    //     "activity_rule_id": "3",
    //     "split_original_amount": "8197.0000",
    //     "sku_id": "11",
    //     "date_id": "2022-09-01",
    //     "source_type_name": "用户查询",
    //     "user_id": "150",
    //     "province_id": "20",
    //     "source_type_code": "2401",
    //     "row_op_ts": "2022-09-17 02:15:56.439Z",
    //     "activity_id": "2",
    //     "sku_name": "Apple iPhone",
    //     "id": "1182",
    //     "order_id": "514",
    //     "split_activity_amount": "200.0",
    //     "split_total_amount": "7997.0",
    //     "ts": "1663380955"
    // }
    
    // 按照唯一键 order_detail_id 进行分组
    // 第一次keyby来进行去重
    .keyBy(
        r -> r.getString("id")
    )
    // flink状态编程 + 定时器去重
    .process(
        // key, in, out
        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            // 状态的声明
            // 放流中的第一条数据, 或者是最后一条数据
            private ValueState<JSONObject> lastValueState;

            open() {
                ValueStateDescriptor<JSONObject> valueStateDescriptor =
                    new ValueStateDescriptor<JSONObject>(
                        "lastValueState",
                        JSONObject.class
                    );

                // 状态的初始化
                lastValueState = getRuntimeContext()
                    .getState(
                        valueStateDescriptor
                    );
            }

            // 对流中数据一条条处理
            processElement(JSONObject jsonObj, Context context, Collector<JSONObject> out) {
                JSONObject lastJsonObj = lastValueState.value();

                // 放第一条数据
                if (lastJsonObj == null) {
                    lastValueState.update(jsonObj);
                    long currentProcessingTime = ctx.timerService().currentProcessingTime();
                    ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                } else {
                    // 同一个明细id已经存在了多条了, 如果状态中已经存在了数据 ，用当前数据的时间戳和状态中的数据时间戳进行比较，
                    
                    // 将时间戳大的保留到状态中   "row_op_ts":"2022-09-14 02:05:30.085Z"
                    String lastRowOpTs = lastJsonObj.getString("row_op_ts");
                    String curRowOpTs = jsonObj.getString("row_op_ts");

                    // 数据格式 2022-04-01 10:20:47.302Z
                    // 1. 去除末尾的时区标志，'Z' 表示 0 时区
                    // "row_op_ts":"2022-09-14 02:05:30.085Z" 明显这个大一些, 如果不去掉z, 就会误认为下面的大, 所以这里去掉时区标记
                    // "row_op_ts":"2022-09-14 02:05:30.08Z"
                    String cleanedTime1 = lastRowOpTs.substring(0, curRowOpTs.length() - 1);
                    String cleanedTime2 = curRowOpTs.substring(0, curRowOpTs.length() - 1);

                    // 2. 比较时间
                    // 如果说数据过来的过快， 两者时间戳是一样的话, 也应该将后面的进行保留
                    if (cleanedTime1.compareTo(cleanedTime2) <= 0) {
                        lastValueState.update(jsonObj);
                    }
                }
            }

            // 定时器去重
            onTimer(long timestamp, OnTimerContext context, Collector<JSONObject> out) {
                JSONObject jsonObj = lastJsonObjState.value();

                if (jsonObj != null) {
                    out.collect(jsonObj);
                }
                
                // 数据往下传递之后, 状态里面数据就没必要保存了
                lastValueState.clear();
            }
        }
    )
    // 类型转换 -> jsonObj转换为实体类对象 -> 方便后面作聚合

    /*--------------------------------------------- TradeProvinceOrderBean ----------------------------------------------------*/

    @Data
    @AllArgsConstructor
    @Builder
    TradeProvinceOrderBean {
        // 窗口起始时间
        String stt;
        // 窗口结束时间
        String edt;
        // 省份 ID
        String provinceId;
        // 省份名称
        @Builder.Default
        String provinceName = "";
        // 累计下单次数
        Long orderCount;
        // 订单 ID 集合，用于统计下单次数
        @TransientSink
        Set<String> orderIdSet;
        // 累计下单金额
        Double orderAmount;
        // 时间戳
        Long ts;
    }

    /*--------------------------------------------- TradeProvinceOrderBean ----------------------------------------------------*/

    .map(
        new MapFunction<JSONObject, TradeProvinceOrderBean>() {
            TradeProvinceOrderBean map(JSONObject jsonObj) {
                String provinceId = jsonObj.getString("province_id");
                Long ts = jsonObj.getLong("ts") * 1000;
                Double splitTotalAmount = jsonObj.getDouble("split_total_amount");
                String orderId = jsonObj.getString("order_id");

                TradeProvinceOrderBean orderBean = TradeProvinceOrderBean
                    .builder()
                    .provinceId(provinceId)
                    .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                    .orderAmount(splitTotalAmount)
                    .ts(ts)
                    .build();
                return orderBean;
            }
        }
    )
    // 指定水位线以及提取事件时间字段
    .assignTimestampsAndWatermarks(
        WaterMarkStrategy
            .<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner(
                new SerializationTimestampAssigner<TradeProvinceOrderBean>() {
                    long extractTimestamp(TradeProvinceOrderBean orderBean, long timestamp) {
                        return orderBean.getTs();
                    }
                }
            )
    )
    // 按照省份分组
    .keyBy(TradeProvinceOrderBean::getProvinceId)
    // 开窗
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    // 聚合
    .reduce(
        new ReduceFunction<TradeProvinceOrderBean>() {
            TradeProvinceOrderBean reduce(TradeProvinceOrderBean acc, TradeProvinceOrderBean in) {

                acc.getOrderIdSet().addAll(in.getOrderIdSet());
                acc.setOrderAmount(acc.getOrderAmount() + in.getOrderAmount());

                return acc;
            }
        },
        // in, out, key, window
        new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
            apply(
                String key,
                TimeWindow window, 
                // 这里窗口聚合之后只有一个结果, 也就是只有一个元素
                Iterable<TradeProvinceOrderBean> input, 
                Collector<TradeProvinceOrderBean> out
            ) {
                String start = DateTimeFormatter
                    .ofPattern("yyyy-MM-dd HH:mm:ss")
                    .format(
                        LocalDateTime
                            .ofInstant(
                                new Date(window.getStart()).toInstant(),
                                ZoneId.systemDefault()
                            )
                    );

                String end = DateTimeFormatter
                    .ofPattern("yyyy-MM-dd HH:mm:ss")
                    .format(
                        LocalDateTime
                            .ofInstant(
                                new Date(window.getEnd()).toInstant(),
                                ZoneId.systemDefault()
                            )
                    );

                for (TradeProvinceOrderBean orderBean : input) {
                    orderBean.setStt(start);
                    orderBean.setEdt(end);
                    orderBean.setTs(System.currentTimeMillis());
                    orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                    out.collect(orderBean);
                }
            }
        }
    );

// 和省份的维度进行关联
// 这里只有省份id, 没有省份名称, 把省份名称关联进来
AsyncDataStream
    .unorderedWait(
        reduceDS,
        new DimAsyncFunction<TradeProvinceOrderBean>("dim_base_province") {
            join(TradeProvinceOrderBean orderBean, JSONObject dimInfoJsonObj) {
                orderBean.setProvinceName(dimInfoJsonObj.getString("NAME"));
            }

            String getKey(TradeProvinceOrderBean orderBean) {
                return orderBean.getProvinceId();
            }
        },
        60, TimeUnit.SECONDS
    )
    .addSink(
        MyClickhouseUtil.getSinkFunction("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)")
    );

env.execute();

/*--------------------------------------------- Clickhouse建表语句 ----------------------------------------------------*/

drop table if exists dws_trade_province_order_window;

create table if not exists dws_trade_province_order_window
(
  stt           DateTime,
  edt           DateTime,
  province_id   String,
  province_name String,
  order_count   UInt64,
  order_amount  Decimal(38, 20),
  ts            UInt64
) engine = ReplacingMergeTree(ts)
    partition by toYYYYMMDD(stt)
    order by (stt, edt, province_id);

/*--------------------------------------------- Clickhouse建表语句 ----------------------------------------------------*/

/*--------------------------------------------- 操作Clickhouse的工具类 ----------------------------------------------------*/

public class MyClickhouseUtil {
    public static <T>SinkFunction<T> getSinkFunction(String sql){
        // flink给那些遵循jdbc规范的数据库专门提供了一个类JdbcSink, 其中.sink()方法的返回值就是SinkFunction
        // 他的问题是, 只能将流数据同时往一张表写, 不能同时往多张表写, 而咱们这里只需要往一张表写
        // <T> -> <KeywordBean>
        SinkFunction<T> sinkFunction = JdbcSink.<T>sink(
            // insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)
            sql,
            new JdbcStatementBuilder<T>() {
                @Override
                public void accept(PreparedStatement ps, T obj) throws SQLException {
                    // 这个方法就是用来给问号占位符赋值的

                    // 现在是不知道有多少个占位符, 也不知道流中对象是什么? 那么现在怎么把对象的属性给对应的占位符赋值呢? 
                    
                    // 使用反射！！！

                    // 获取类中的所有属性
                    // obj.getClass()获取当前对象的所属类型
                    // .getFields()会获取从父类继承下来的属性, .getDeclaredFields()获取当前类中定义的属性包括私有
                    // 属性数组
                    Field[] fieldArr = obj.getClass().getDeclaredFields();
                    // 对类中的属性进行遍历
                    int skipNum = 0;
                    for (int i = 0; i < fieldArr.length; i++) {
                        //获取一个属性对象
                        Field field = fieldArr[i];

                        //判断当前属性是否需要向CK保存
                        // KeywordBean {
                        //     String stt, -> 窗口起始时间
                               @TransientSink -> 假设这个属性不需要写到ck中, 当然咱们这里不用写, 因为每个属性都需要
                        //     String edt, -> 窗口闭合时间
                        //     String source, -> 关键词来源
                        //     String keyword, -> 关键词
                        //     Long keyword_count, -> 关键词出现频次
                        //     Long ts -> 时间戳
                        // }

                        /*-------------------------------------------------------------------------------------------------*/

                        自定义注解用来标记不需要向clickhouse保存的属性

                        // 元注解 -> 标记注解的注解 -> 注解加的地方 -> 字段上
                        @Target(ElementType.FIELD)
                        // 注解的作用范围 -> 整个程序运行的时候有作用
                        @Retention(RetentionPolicy.RUNTIME)
                        public @interface TransientSink {}

                        /*-------------------------------------------------------------------------------------------------*/

                        // 获取属性上标注的注解TransientSink
                        TransientSink transientSink = field.getAnnotation(TransientSink.class);
                        if(transientSink != null){
                            skipNum++;

                            // 不需要写ck就不执行后面的操作了, 使用continue进入for的下一个循环
                            continue;
                        } 

                        // 因为一般封装的属性都是私有化的, 所以咱们这里还要设置一下私有属性的访问权限
                        //设置私有属性的访问权限, 那么现在就可以访问私有属性了
                        field.setAccessible(true);
                        try {
                            // field里面包括属性的类型, 属性的名称, 属性的值
                            // 这里获取属性的值
                            Object filedValue = field.get(obj);
                            // 将属性的值给对应的问号占位符赋上

                            // 注意这里就要保证表中字段的顺序就需要和类属性顺序保持一致了

                            ps.setObject(i + 1 - skipNum, filedValue);
                        } catch (IllegalAccessException e) {
                            // 这里是对field.get()这个方法做异常处理
                            e.printStackTrace();
                        }
                    }
                }
            },
            new JdbcExecutionOptions.Builder()
                // 流中来一条数据就与clickhouse建立连接插入表中, 交互太频繁, 咱们这里可以缓存数据条数, 攒一批
                // 这里5条是单个并行度到5条
                .withBatchSize(5)
                // 假设某个并行度一直是3条, 到不了5条, 这里设置一下时间
                .withBatchIntervalMs(3000)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                // 连接clickhouse的驱动名
                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                // 连接clickhouse的url
                // 数据放在默认库default中
                .withUrl("jdbc:clickhouse://hadoop202:8123/default")
                .build()
        );
        return sinkFunction;
    }
}

/*-------------------------------------------------------------------------------------------------*/