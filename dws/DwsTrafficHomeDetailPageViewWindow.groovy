DwsTrafficHomeDetailPageViewWindow

-> 流量域首页、详情页独立访客的聚合计算
    
    注意: 直接从这个主题dwd_traffic_unique_visitor_detail过滤出首页、详情页独立访客会丢数据
          mid01某天先访问了首页, 首页算独立访客, 如果接着他又访问了详情页, 那么它就不属于独立访客了

    咱们这里想统计的是访问首页的独立访客有哪些, 访问详情页的独立访客有哪些

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
env
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
            // props.setProperty("group.id", "dws_traffic_home_detail_group");

            props
        }
    )
    // 将String类型数据转换为JSONObject类型
    .map(JSON::parseObject)
    // 过滤出首页以及详情页日志
    .filter(
        new FilterFunction<JSONObject>() {
            boolean filter(JSONObject jsonObj) {
                String pageId = jsonObj.getJSONObject("page").getString("page_id");

                if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                    return true;
                }
                return false;
            }
        }
    )
    // 指定水位线以及提取事件时间字段
    .assignTimestampAndWatermarks(
        WaterMarkStrategy
            .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner(
                new SerializationTimestampAssigner<JSONObject>() {
                    long extractTimestamp(JSONObject jsonObj, long timestamp) {
                        return jsonObj.getLong("ts");
                    }
                }
            )
    )
    // 按照mid分组
    .keyBy(
        r -> r.getJSONObject("common").getString("mid")
    )
    // 使用flink状态编程 判断是否是首页以及详情页的独立访客

    /*--------------------------------------------- TrafficHomeDetailPageViewBean ----------------------------------------------------*/

    TrafficHomeDetailPageViewBean {
        String stt;             -> 窗口起始时间
        String edt;             -> 窗口结束时间
        Long homeUvCt;          -> 首页独立访客数
        Long goodDetailUvCt;    -> 商品详情页独立访客数
        Long ts;                -> 时间戳
    }

    /*--------------------------------------------- TrafficHomeDetailPageViewBean ----------------------------------------------------*/

    .process(
        // key, in, out
        new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
            // 状态的声明
            private ValueState<String> homeLastVisitDateState;
            private ValueState<String> detailLastVisitDateState;

            open() {
                ValueStateDescriptor<String> homeValueStateDescriptor =
                    new ValueStateDescriptor<String>(
                        "homeLastVisitDateState",
                        String.class
                    );
                
                // 统计的当天的独立访客, 状态没必要一直保存, 所以这里状态只保存一天
                homeValueStateDescriptor.enableTimeToLive(
                    StateTtlConfig
                        .newBuilder(Time.days(1))
                        .build()
                );
                
                // 状态的初始化
                homeLastVisitDateState = getRuntimeContext()
                    .getState(
                        homeValueStateDescriptor
                    );


                ValueStateDescriptor<String> detailValueStateDescriptor =
                    new ValueStateDescriptor<String>(
                        "detailLastVisitDateState",
                        String.class
                    );
                
                detailValueStateDescriptor.enableTimeToLive(
                    StateTtlConfig
                        .newBuilder(Time.days(1))
                        .build()
                );
                
                // 状态的初始化
                detailLastVisitDateState = getRuntimeContext()
                    .getState(
                        detailValueStateDescriptor
                    );
            }

            // 对流中数据一条条处理
            processElement(JSONObject jsonObj, Context context, Collector<TrafficHomeDetailPageViewBean> out) {
                // 通过拿到pageId来知道是首页还是详情页
                String pageId = jsonObj.getJSONObject("page").getString("page_id");
                Long ts = getLong("ts");
                String curVisitDate = DateTimeFormatter
                    .ofPattern("yyyy-MM-dd")
                    .format(
                        LocalDateTime
                            .ofInstant(
                                new Date(ts).toInstant(),
                                ZoneId.systemDefault
                            )
                    );

                Long homeUvCt = 0L;
                Long detailUvCt = 0L;

                if ("home".equals(pageId)) {
                    String homeLastVisitDate = homeLastVisitDateState.value();
                    
                    // 当天访问日期为空 或者 本次访问日期与上次访问日期不是同一天, 属于独立访客
                    if (StringUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                        homeUvCt = 1L;

                        // 当前访问日期更新到状态中去
                        homeLastVisitDateState.update(curVisitDate);
                    }
                }

                if ("good_detail".equals(pageId)) {
                    String detailLastVisitDate = detailLastVisitDateState.value();

                    if (StringUtils.isEmpty(detailLastVisitDate) || !detailLastVisitDate.equals(curVisitDate)) {
                        detailUvCt = 1L;
                        detailLastVisitDateState.update(curVisitDate);
                    }
                }

                // 若是首页或者详情页的独立访客, 往下游发送
                if (homeUvCt != 0L || detailUvCt != 0L) {
                    out.collect(new TrafficHomeDetailPageViewBean(
                        // 由于没有开窗, 所以窗口的起始和结束时间没有办法给你
                        "",
                        "",
                        homeUvCt,
                        detailUvCt,
                        ts
                    ));
                }
            }
        }
    )
    // 开全窗口
    .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
    // 聚合
    .reduce(
        new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean acc, TrafficHomeDetailPageViewBean in) {
                acc.setHomeUvCt(acc.getHomeUvCt() + in.getHomeUvCt());
                acc.setGoodDetailUvCt(acc.getGoodDetailUvCt() + in.getGoodDetailUvCt());
                return acc;
            }
        },
        // in, out, window
        new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            apply(
                TimeWindow window, 
                Iterable<TrafficHomeDetailPageViewBean> values, 
                Collector<TrafficHomeDetailPageViewBean> out
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

                for (TrafficHomeDetailPageViewBean viewBean : values) {
                    viewBean.setStt(start);
                    viewBean.setEdt(end);
                    viewBean.setTs(System.currentTimeMillis());

                    out.collect(viewBean);
                }
            }
        }
    )
    // 将聚合结果写到ClickHouse
    .addSink(
        MyClickhouseUtil.getSinkFunction("insert into dws_traffic_home_detail_page_view_window values(?,?,?,?,?)")
    );

env.execute();

/*--------------------------------------------- Clickhouse建表语句 ----------------------------------------------------*/

drop table if exists dws_traffic_home_detail_page_view_window;

create table if not exists dws_traffic_home_detail_page_view_window
(
  stt               DateTime,
  edt               DateTime,
  home_uv_ct        UInt64,
  good_detail_uv_ct UInt64,
  ts                UInt64
) engine = ReplacingMergeTree(ts)
    partition by toYYYYMMDD(stt)
    order by (stt, edt);

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
