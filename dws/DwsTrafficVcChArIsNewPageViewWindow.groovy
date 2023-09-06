DwsTrafficVcChArIsNewPageViewWindow

-> 流量域: 版本、渠道、地区、访客类别粒度 -> 页面浏览各窗口轻度聚合

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
    String stt,     -> 窗口起始时间
    String edt,     -> 窗口结束时间
    String vc,      -> app版本号
    String ch,      -> 渠道
    String ar,      -> 地区
    String isNew,   -> 新老访客状态标记
    Long uvCt,      -> 独立访客数
    Long svCt,      -> 会话数
    Long pvCt,      -> 页面浏览数
    Long durSum,    -> 累计访问时长
    Long ujCt,      -> 跳出会话数
    Long ts         -> 时间戳
}

// 对读取的独立访客日志数据jsonStr转换为实体类对象
plStatsDS = plStrDS
    .map(
        new MapFunction<String, TrafficPageViewBean>() {
            TrafficPageViewBean map(String str) {
                // json字符串转换为json对象
                JSONObject jsonObj = JSON.parseObject(str);

                // 取common、page、ts
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

                // 取common、ts
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

                // 取common、ts
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
    .keyBy(r -> Tuple4.of(
        r.getVc(),
        r.getCh(),
        r.getAr(),
        r.getIsNew()
    ))
    // 开窗
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    // 聚合
    