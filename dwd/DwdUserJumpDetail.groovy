DwdUserJumpDetail

// 流执行环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 设置并行度, 与kafka分区数保持一致
env.setParallelism(4);

// 开启检查点, 设置检查点时间为3秒, 这是检查点模式为精准一次
env.enableCheckpointing(3000L, CheckpointMode.EXACTLY_ONCE);
// 设置检查点超时时间为1min
env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
// 设置两次检查点之间的最小间隔
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
// 设置取消任务时保留检查点
env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
// 设置重启策略为失败率重启, 有效时间为1天, 每天3次机会, 每两次间隔时间为1min
env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.minutes(1)));
// 设置状态后端
// 设置状态在taskmanager堆内存中的保存形式为HashMapState
env.setStateBackend(new HashMapStateBackend());
// 指定状态的远程存储路径
env.getCheckpointConfig().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");
// 指定hdfs操作用户
System.setProperty("HADOOP_USER_NAME", baiyao);

env
    // 将flinkKafkaConsumer<String>封装为流
    .addSource(
        new FlinkKafkaConsumer<String>(
            // 从页面日志topic读取数据
            "dwd_traffic_page_log",
            // kafka反序列化
            new KafkaDeserializationSchema<String>() {
                boolean isEndOfStream() return false;

                String serialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
                    if (consumerRecord != null || consumerRecord.value() != null) {
                        return new String(consumerRecord.value());
                    }
                    return null;
                }

                TypeInformation<String> getProducedType() {
                    return TypeInformation.of(String.class);
                }
            },
            // Properties props = new Properties();
            // props.setProperty("bootstrap.servers", "zoo1:9092");
            // props.setProperty("group.id", "dwd_traffic_user_jump_group");
            props
        ) 
    )
    // String转换为JSONObject
    .map(JSON::parseObject)
    // 指定水位线生成策略以及指定事件时间字段
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner(
                long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                    return jsonObj.getLong("ts");
                }
            )
    )
    // 按照mid进行分组
    .keyBy(r -> r.getJOSNObject("common").getString("mid"))

-> KeyedStream<JSONObject, String> keyedDS

    // 用flinkCEP判断是否为跳出行为
    // 定义Pattern
    Pattern<JSONObject, JSONObject> pattern = Pattern
        // 第一个匹配事件
        .<JSONObject>begin("first")
        .where(
            new SimpleCondition<JSONObject>() {
                boolean filter(JSONObject jsonObj) {
                    return StringUtils.isEmpty(jsonObj.getJSONObject("page").getString("last_page_id"));
                }
            }
        )
        // 第二个匹配事件
        .next("second")
        .where(
            new SimpleCondition<JSONObject>() {
                boolean filter(JSONObject jsonObj) {
                    return StringUtils.isEmpty(jsonObj.getJSONObject("page").getString("last_page_id"));
                }
            }
        )
        // 两次事件匹配时间间隔
        .within(Time.seconds(10));

// 定义超时数据侧输出流
OutputTag timeoutTag = new OutputTag("timeoutTag"){};

    SingleOutputStreamOperator<String> matchDS = CEP
        // 将定义的pattern应用到流上
        .pattern(keyedDS, pattern)
        .select(
            timeoutTag,
            new PatternTimeoutFunction<JSONObject, String>() {
                // 这里会把超时的数据放到侧输出流里面去
                String timeout(Map<String, List<JSONObject>> pattern, long TimeoutTimestamp) {
                    return pattern.get("first").get(0).toJSONString();
                }
            },
            new PatternSelectFunction<JSONObject, String>() {
                // 匹配数据放入主流
                String select(Map<String, List<JSONObject>> pattern) {
                    return pattern.get("first").get(0).toJSONString();
                } 
            }
        )

// 合并超时数据与匹配的数据
matchDS
    .union(matchDS.getSideOutput(timeoutTag))
    // 写入kafka主题
    .addSink(
        new FlinkKafkaProducer<String>(
            "default_topic",
            new KafkaSerializationSchema<String>() {
                ProducerRecord<byte[], byte[]> serialize(String str) {
                    return new ProducerRecord<byte[], byte[]>("dwd_traffic_user_jump_detail", str.getBytes());
                }
            },
            // Properties props = new Properties();
            // props.setProperty("bootstrap.servers", "zoo1:9092");
            // props.setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "");
            props,
            FlnkKafkaConsumer.Semantic.EXACTLY_ONCE
        ) 
    )

env.execute();

