DwdTrafficUniqueVisitorDetail

// 流执行环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 设置并行度
env.setParallelism(4);

// 开启检查点, 设置检查点时间为3秒, 设置检查点模式为精准一次
env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
// 设置检查点超时时间为1min
env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
// 设置两次检查点之间最小时间间隔为3秒
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
// 设置取消任务时是否保留外部检查点
env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
// 设置失败重启策略为失败率重启, 失效为一天, 一天3次, 每两次重启间隔为1min
env.setRestartStrategy.RestartStrategies.failureRateRestart(3, Time.days(1), Time.minutes(1));
// 设置状态后端
// 设置状态保存在taskmanager内存中的形式为hashmap
env.setStateBackend(new HashMapStateBackend());
// 设置检查点远端备份路径
env.getCheckpointConfig().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");
// 设置操作hdfs的用户
System.setProperty("HADOOP_USER_NAME", baiyao);

// FlinkKafkaConsumer读取topic_log日志
Properties props = new Properties();
props.setProperty("bootstrap.servers", "zoo1:9092");
props.setProperty("group.id", "dwd_traffic_unique_visitor_detail_gtoup");

env
    .addSource(
        new FlinkKafkaConsumer<String>(
            "topic_log",
            new KafkaDeserializationSchema<String>() {
                boolean isEndOfStream() return false;

                String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
                    if (consumerRecord != null && consumerRecord.value() != null)
                        return new String(consumerRecord.value());
                    return null;
                } 

                getProducedType() return TypeInformation.of(String.class);
            },
            props
        )
    )
    // 将topic_log中的String类型转换为JSONObject类型
    .map(JSON::parseObject)
    // 按照mid分组
    .keyBy(r -> r.getJSONObject("common").getString("mid"))
    // 使用状态过滤独立访客
    .filter(
        new RichFilterFunction<JSONObject>() {
            // 值状态变量存储状态日期
            ValueState<String> lastVisitDateState;

            open() {
                // 声明值状态
                ValueStateDescriptor lastVisitDateDescriptor = new ValueStateDescriptor<String>(
                    "lastVisitDateState",
                    String.class
                )

                // 设置状态有效时间为1天
                lastVisitDateDescriptor.enableTimeToLive(
                    StateTtlConfig
                        // 通过静态内部类构造外部类对象, 设置生效时间是1天
                        .newBuilder(Time.days(1))
                        // 在状态创建或者变更的时候, 生效时间会重新计时
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        // 状态过了生效时间后, 有可能并未被清理, 那么此时即便有状态访问, 也不返回状态值
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();
                )

                // 值状态赋值
                lastVisitDateState = getRuntimeContext().getState(lastVisitDateDescriptor);
            }

            boolean filter(JSONObject jsonObj) {

                if (StringUtils.isNotEmpty(jsonObj.getJSONObject("page").getString("last_page_id"))) {
                    return false;
                }

                String lastVisitDate = lastVisitDateState.value();
                String currentVisitDate = DateTimeFormatter
                    .ofPattern("yyyy-MM-dd")
                    .format(
                        LocalDateTime
                            .ofInstant(
                                new Date(jsonObj.getLong("ts")).toInstant,
                                ZoneId.SystemDefault
                            )
                    );
                
                if (StringUtils.isEmpty(lastVisitDate) || !currentVisitDate.equals(lastVisitDate)) {
                    lastVisitDateState.update(currentVisitDate);
                    return true;
                }

                return false;
            }
        }
    )
    // 写入kafka前将JSONObject转化为String类型
    .map(r -> r.toJSONString())
    // 写入kafka
    .addSink(
        new FlinkKafkaProducer<String>(
            // flinkKafkaProducer发送的默认topic
            "default_topic",
            // 数据序列化实现
            new KafkaSerializationSchema<String>() {
                ProducerRecord<byte[], byte[]> serialize(String str) {
                    return new ProducerRecord<byte[], byte[]>(
                        "dwd_traffic_unique_visitor_detail", 
                        str.getBytes()
                    )
                }
            },
            // 需要给定kafka集群地址以及事务的超时时间
            // Properties props = new Properties();
            // props.setProperty("bootstrap.servers", "zoo1:9092");
            // props.setProperty("transaction.timeout.ms", 15 * 60 * 1000L);
            props,
            // 指定语义为精准一次进而开启事务
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        )
    )

env.execute();