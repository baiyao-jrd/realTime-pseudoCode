DwsTrafficVcChArIsNewPageViewWindow

-> 流量域: 版本、渠道、地区、访客类别粒度 -> 页面浏览各窗口轻度聚合

    维度: 版本、渠道、地区、新老访客
    度量: 
        // dwd_traffic_page_log
        页面数pv、持续访问时间dur、会话数量sv、
        // dwd_traffic_unique_visitor_detail
        独立访客数uv、
        // dwd_traffic_user_jump_detail
        跳出数uj

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

// 创建消费者对象消费dwd_traffic_page_log主题并封装为流
plStrDS = env
    .addSource(
        new FlinkKafkaConsumer<String>() {
            "dwd_traffic_page_log",
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
            // props.setProperty("group.id", "dws_traffic_vc_ch_group");

            props
        }
    );

// 创建消费者对象消费dwd_traffic_unique_visitor_detail主题并封装为流
uvStrDS = env
    .addSource(
        new FlinkKafkaConsumer<String>() {
            "dwd_traffic_unique_visitor_detail",
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
            // props.setProperty("group.id", "dws_traffic_vc_ch_group");

            props
        }
    );

// 创建消费者对象消费dwd_traffic_user_jump_detail主题并封装为流
ujStrDS = env
    .addSource(
        new FlinkKafkaConsumer<String>() {
            "dwd_traffic_user_jump_detail",
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
            // props.setProperty("group.id", "dws_traffic_vc_ch_group");

            props
        }
    );

// 实体类对象 -> TrafficPageViewBean
TrafficPageViewBean {
    String stt,     -> 窗口起始时间      -> 时间相关
    String edt,     -> 窗口结束时间      -> 时间相关
    String vc,      -> app版本号        -> 维度
    String ch,      -> 渠道             -> 维度
    String ar,      -> 地区             -> 维度
    String isNew,   -> 新老访客状态标记  -> 维度

    Long uvCt,      -> 独立访客数       -> 度量
    Long svCt,      -> 会话数           -> 度量
    Long pvCt,      -> 页面浏览数       -> 度量
    Long durSum,    -> 累计访问时长     -> 度量
    Long ujCt,      -> 跳出会话数       -> 度量
    Long ts         -> 时间戳           -> 时间相关
}

// 对读取的独立访客日志数据jsonStr转换为实体类对象
plStatsDS = plStrDS
    .map(
        new MapFunction<String, TrafficPageViewBean>() {
            TrafficPageViewBean map(String str) {
                // json字符串转换为json对象
                JSONObject jsonObj = JSON.parseObject(str);

                // 取common、page、ts(13位时间戳)
                JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                Long ts = jsonObj.getLong("ts");

                return new TrafficPageViewBean(
                    "",
                    "",
                    commonJsonObj.getString("vc"),
                    commonJsonObj.getString("ch"),
                    commonJsonObj.getString("ar"),
                    commonJsonObj.getString("is_new"),
                    0L,
                    StringUtils.isEmpty(pageJsonObj.getString("last_page_id")) ? 1L : 0L,
                    1L,
                    pageJsonObj.getLong("during_time"),
                    0L,
                    ts
                )
            }
        }
    );

// 对读取的页面日志数据jsonStr转换为实体类对象
uvStatsDS = uvStrDS
    .map(
        new MapFunction<String, TrafficPageViewBean>() {
            TrafficPageViewBean map(String str) {
                // json字符串转换为json对象
                JSONObject jsonObj = JSON.parseObject(str);

                // 取common、ts(13位时间戳)
                JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                Long ts = jsonObj.getLong("ts");

                return new TrafficPageViewBean(
                    "",
                    "",
                    commonJsonObj.getString("vc"),
                    commonJsonObj.getString("ch"),
                    commonJsonObj.getString("ar"),
                    commonJsonObj.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    ts
                )
            }
        }
    );

// 对读取的用户跳出日志数据jsonStr转换为实体类对象
ujStatsDS = ujStrDS
    .map(
        new MapFunction<String, TrafficPageViewBean>() {
            TrafficPageViewBean map(String str) {
                // json字符串转换为json对象
                JSONObject jsonObj = JSON.parseObject(str);

                // 取common、ts(13位时间戳)
                JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                Long ts = jsonObj.getLong("ts");

                return new TrafficPageViewBean(
                    "",
                    "",
                    commonJsonObj.getString("vc"),
                    commonJsonObj.getString("ch"),
                    commonJsonObj.getString("ar"),
                    commonJsonObj.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    0L,
                    1L,
                    ts
                )
            }
        }
    );

plStatsDS
    // 合并三条流
    .union(uvStateDS, ujStateDS)
    // 指定Watermark以及提取事件时间字段
    .assignTimestampsAndWatermarks(
        WaterMarkStrategy
            .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner(
                new SerializationTimestampAssigner<TrafficPageViewBean>() {
                    long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long timestamp) {
                        return trafficPageViewBean.getTs();
                    }
                }
            )
    )
    // 分组
    // 现在维度有4个: 版本、渠道、地区、新老访客
    // 按照4个维度进行分组
    .keyBy(r -> Tuple4.of(
        r.getVc(),
        r.getCh(),
        r.getAr(),
        r.getIsNew()
    ))
    // 开窗
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))

    /*--------------------------------------------- 对窗口作聚合计算的总结 ----------------------------------------------------*/

    // 下面两个是对窗口元素作处理
    1. ds.process(
            new ProcessWindowFunction<in, out, key, window>() {
                process(key, context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) {
                    // 这个方法是把窗口中的所有元素放到了一个可迭代的集合Iterable<TrafficPageViewBean> elements里面去了
                    // 如果要对窗口元素进行处理的话只需要把窗口元素遍历出来就行了

                    // 通过out继续向下游传递
                }
            } 
       );

    2. ds.apply(
        new WindowFunction<in, out, key, window>() {
            apply(key, timewindow, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) {
                // 这个方法也是把窗口中的所有元素放到了一个可迭代的集合Iterable<TrafficPageViewBean> elements里面去了
                // 如果要对窗口元素进行处理的话只需要把窗口元素遍历出来就行了

                // 通过out继续向下游传递


                注意与上面的不同: process -> context(上下文对象) -> context.window() 可获得窗口对象 -> 明显process更底层一些
                                 apply   -> timeWindow(窗口对象)
            }
        } 
    );
    // 下面是对窗口元素作聚合计算
    3. ds.reduce(
            new ReduceFunction<TrafficPageViewBean>() {
                TrafficPageViewBean reduce(acc, in) {
                    return acc;
                }
            }
       );
    
    4. ds.aggregate(
            new AggreateFunction<in, acc, out>() {
                // 创建累加器
                TrafficPageViewBean createAccumulator() {
                    return null;
                }

                // 聚合计算
                TrafficPageViewBean add(in, acc) {
                    return null;
                }

                // 获取聚合结果
                TrafficPageViewBean getResult(acc) {
                    return acc;
                }

                // 对聚合结果作合并 -> 这个只是会话窗口需要重写
                TrafficPageViewBean merge(a, b) { return null; }
            }
       );

    reduce -> 输入数据和累加器类型一致 -> 方便
    aggregate -> 输入数据类型与累加器类型可以不一样 -> 更灵活

    /*--------------------------------------------- 对窗口作聚合计算的总结 ----------------------------------------------------*/

    // 聚合
    .reduce(
        new ReduceFunction<TrafficPageViewBean>() {
            TrafficPageViewBean reduce(TrafficPageViewBean acc, TrafficPageViewBean in) {

                acc.setPvCt(acc.getPvCt() + in.getPvCt());
                acc.setDurSum(acc.getDurSum() + in.getDurSum());
                acc.setSvCt(acc.getSvCt() + in.getSvCt());
                acc.setUvCt(acc.getUvCt() + in.getUvCt());
                acc.setUjCt(acc.getUjCt() + in.getUjCt());

                return acc;
            }
        },
        // in, out, key, window
        // 经过上面的reduce, 维度属性有了, 窗口的起始结束时间还没有指定, 下面就需要再调用WindowFunction -> 调apply
        // 或者ProcessWindowFunction -> 调process
        // 来包装一层窗口信息
        new windowFunction<>(TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow) {
            // key, window, iterable, collector
            apply(
                Tuple4>String, String, String, String> key,
                TimeWindow window,
                // 咱们现在迭代器集合里面只有一个聚合后的元素
                Iterable<TrafficPageViewBean> input,
                Collector<TrafficPageViewBean> out
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

                for (TracfficPageViewBean viewBean : input) {
                    viewBean.setStt(start);
                    viewBean.setEdt(end);
                    // 13位时间戳
                    viewBean.setTs(System.currentTimeMillis());
                    out.collect(viewBean);
                }
            }
        }
    )
    // 将聚合结果写到ClickHouse
    .addSink(
        MyClickhouseUtil.getSinkFunction("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)")
    );

env.execute();

/*--------------------------------------------- Clickhouse建表语句 ----------------------------------------------------*/

drop table if exists dws_traffic_vc_ch_ar_is_new_page_view_window;
create table if not exists dws_traffic_vc_ch_ar_is_new_page_view_window
(
  stt     DateTime,
  edt     DateTime,
  vc      String,
  ch      String,
  ar      String,
  is_new  String,
  uv_ct   UInt64,
  sv_ct   UInt64,
  pv_ct   UInt64,
  dur_sum UInt64,
  uj_ct   UInt64,
  ts      UInt64
) engine = ReplacingMergeTree(ts)
    partition by toYYYYMMDD(stt)
    order by (stt, edt, vc, ch, ar, is_new);

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
