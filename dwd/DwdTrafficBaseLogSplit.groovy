DwdTrafficBaseLogSplit

// 1. 本地环境准备
// 指定流环境
StreamExecutionEnvironment env = StreanmExecutionEnvironment.getExecutionEnvironment();
// 设置并行度为4, 保证与kafka分区数一致
env.setParallelism(4);

// 2. 设置状态后端
// 开启检查点、设置检查点时间为3秒以及检查点模式为精准一次
env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
// 设置检查点超时时间为一分钟
env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
// 设置两次检查点之间的最小时间间隔为3秒
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
// 设置取消任务的时候是否保留外部检查点, 设置为保留
env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
// 设置失败重启策略为故障率重启, 有效时间为1天, 每天重启机会为3次, 每两次间隔时间为1min
env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.minutes(1)));
// 设置状态后端
// 设置状态在taskmanager内存中存储形式为HashMap
env.setStateBackend(new HashMapStateBackend());
// 设置远程检查点备份路径为hdfs
env.getCheckpointConfig().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");
// 设置hdfs操作用户为baiyao
System.setProperty("HADOOP_USER_NAME", baiyao);

// 3. 从kafka的topic_log读取日志数据
Properties props = new Properties();
props.setProperty("bootstrap.servers", "zoo1:9092");
props.setProperty("group.id", "dwd_traffic_base_log_split_group");

FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>() {
    // 指定读取的topic
    "topic_log",
    // kafka反序列化方法, 返回值是String
    new KafkaDeserializationSchema<String>() {
        isEndOfStream return false;

        deserialize(ConsumerRecord<byte [], byte[]> consumerRecord) {
            if (consumerRecord.value() != null) {
                return new String(consumerRecord.value());
            }
            return null;
        }

        getProducedType() return TypeInformation.of(String);
    },
    // kafka集群地址bootstrap.servers以及消费者组group.id
    props
}

// 定义脏数据进入的侧输出流
OutputTag<String> dirtyData = new OutputTag("dirty_data"){};
// 定义错误日志侧输出流
OutputTag<String> errorTag = new OutputTag("errorTag"){};
// 定义启动日志侧输出流
OutputTag<String> startTag = new OutputTag("startTag"){};
// 定义曝光日志侧输出流
OutputTag<String> displayTag = new OutputTag("displayTag"){};
// 定义动作日志侧输出流
OutputTag<String> actionTag = new OutputTag("actionTag"){};

-> JsonObjDataStream

env
    // 将FlinkkakfaConsumer<String>封装为流
    .addSource(flinkKafkaConsumer)
    // 调process使用processFunction将数据转换为jsonObject格式, 不能转换的放入侧输出流中, 能转换的就转换成json字符串并往下游发送
    .process(
        new ProcessFunction<String, JSONObject>() {

            processElement(String jsonStr, Context context, Collector<JSONObject> out) {
                try {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);

                    out.collect(jsonObj);
                } catch(Exception e) {
                    e.printStackTrace;

                    context.output(dirty_data, jsonStr);
                }
            }
        }
    )

Properties props = new Properties();
props.setProperty("bootstrap.servers", "zoo1:9092");
// 事务时间阈值一定要比检查点超时时间长来保证检查点做完
props.setProperty("transaction.timeout.ms", 15 * 60 * 1000L);

JsonObjDataStream
    // 获取侧输出流
    .getSideOutput(dirtyData)
    // 将侧输出流数据发送至kafka主题中
    .addSink(
        new FlinkKafkaProducer<String>(
            // 默认发送的topic
            "default_topic",
            // kafka序列化方法
            new KafkaSerializationSchema<String>() {
                ProducerRecord<byte[], byte[]> serialize(String str) {
                    return new ProducerRecord<byte[], byte[]>("dirty_data", str.getBytes());
                }
            },
            // 指定kafka集群地址bootstrap.servers以及事务的超时时间
            props,
            // 指定FlinkKafkaConsumer的语义为精准一次, 以此来保证开启事务机制, 至少一次语义是默认的, 他是不开启事务的
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        )
    );

// 将主流中不能转换为完整json的数据放入侧输出流并存入kafka之后就需要将主流数据按照mid分组了
JsonObjDataStream
    // 将数据按照topic_log中common的mid分组, 确保后面拿到每一个状态的时候都是某个mid的上次访问日期
    .keyBy(r -> r.getJSONObj("common").getString("mid"))
    .map(
        new RichMapFunction<JSONObject, JSONObject>() {
            // 获取某一个mid的值状态
            ValueState<String> lastVisitDateState;

            // 值状态初始化
            open() {
                lastVisitDateState = getRuntimeContext().getState(new valueStateDesciptor<String>(
                    "lastVisitDateState",
                    String.class
                ))
            }

/**
        // 13位时间戳
        // 1693638841043
        System
                .currentTimeMillis();
        // 2023-09-02T07:14:01.044Z
        new Date(
                System
                        .currentTimeMillis()
        ).toInstant();
        // 2023-09-02T15:14:01.143
        LocalDateTime
                .ofInstant(
                        new Date(System.currentTimeMillis()).toInstant(), 
                        ZoneId.systemDefault()
                );
        // 2023-09-02 15:14:01
        DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss")
                .format(
                        LocalDateTime
                                .ofInstant(new Date(System.currentTimeMillis()).toInstant(), 
                                        ZoneId.systemDefault())
                );
*/

            JSONObj map(JSONObj jsonObj) {
                // 获取从kafka的topic_log中拿到的is_new新老访客标记
                String is_new = jsonObj.getJSONObj("common").getString("is_new");
                // 拿到上次访问日期
                String lastVisitDate = lastVisitDateState.value();
                // 拿到数据产生时携带的13位时间戳ts
                Long ts = jsonObj.getLong("ts");

                // 使用时间日期类将13位时间戳转换为2023-09-02的格式
                String visitDate = DateTimeFormatter
                    .ofPattern("yyyy-MM-dd")
                    .format(
                        LocalDateTime
                            .ofInstant(
                                new Date(ts).toInstant(),
                                ZoneId.SystemDefault()
                            )
                    )

                // 拿到新老访客标记is_new:
                // 1: 如果上次访问日期为空, 是新访客, 将访问日期更新到状态中去; 如果上次访问日期不等于当前访问日期, 说明是今天的新访客, 更新状态
                // 0: 如果是实时数仓刚建起来, 状态里面没有上次访问日期的话, 就需要将状态里面的上次访问日期更新为前一天

                if ("1".equals(is_new)) {
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        lastVisitDateState.update(visitDate);
                    } else {
                        if (!lastVisitDate.equals(visitDate)) {
                            jsonObj.getJSONObject("commons").put("is_new", "0");
                        }
                    }
                } else {
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        lastVisitDateState.update(
                            DateTimeFormatter
                                .ofPattern("yyyy-MM-dd")
                                .format(
                                    LocalDateTime
                                        .ofInstant(
                                            new Date(ts - 24 * 60 * 60 * 1000L).toInstant(),
                                            ZoneId.SystemDefault()
                                        )
                                )
                        )
                    }
                }

                return jsonObj;
            }
        }
    )
    .process(
        new ProcessFunction<JSONObject, String>() {
            processElement(JSONObject jsonObj, Context context, Collector<String> collector) {
                // jsonObj中有"err", 就将数据发送至errorTag侧输出流, 最后remove掉"err"
                // jsonObj中有"start", 就将数据发送至startTag侧输出流
                // 从jsonObj中获取页面日志的三个属性: page、common、ts
                //      将jsonObj中的displays切分, 新put("display", displayJSONObject), 得到曝光日志并放入displayTag中
                //      将jsonObj中的actions切分, 新put("action", actionJSONObject), 得到动作日志并放入actionTag中

                JSONObject errJsonObj = jsonObj.getJSONObject(err);    

                if (errJsonObj != null) {
                    context.output(errorTag, jsonObj.toJSONString());
                    jsonObj.remove("err");
                }

                JSONObject startJsonObj = jsonObj.getJSONObject("start");

                if (startJsonObj != null) {
                    context.output(startTag, jsonObj.toJSONString());
                } else {
                    // 页面属性: page, common, ts

                    JSONObject page = jsonObj.getJSONObject("page");
                    JSONObject common = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts");
                    JSONArray displays = jsonObj.getJSONArray("displays");
                    JSONArray actions = jsonObj.getJSONArray("actions");

                    // 删除曝光和动作信息, 将只包含page, common, ts的日志信息往下游发送
                    jsonObj.remove("diplays");
                    jsonObj.remove("actions");

                    // 将曝光信息发送至曝光侧输出流displayTag
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displayJsonObj = new JSONObject()
                            displayJsonObj.put("page", page);
                            displayJsonObj.put("common", common);
                            displayJsonObj.put("ts", ts);
                            displayJsonObj.put("display", diplays.getJSONObject(i));
                            context.output(displayTag, displayJsonObj.toJSONString());
                        }
                    }

                    // 将动作信息发送至动作侧输出流actionTag
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject actionJsonObj = new JSONObject();
                            actionJsonObj.put("page", page);
                            actionJsonObj.put("common", common);
                            actionJsonObj.put("action", actions.getJSONObject(i));

                            collector.collect(actionTag, actionJsonObj.toJSONString());
                        }
                    }

                    collector.collect(jsonObj.toJSONString());
                }


            }
        }
    )

    -> pageDataStream

    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "zoo1:9092");
    props.setProperty("transaction.timeout.ms", 15 * 60 * 1000L);

    // 错误侧输出流数据写入kafka中
    pageDataStream
        .getSideOutput(errorTag)
        .addSink(
            new FlinkKafkaProducer<String> (
                "default_topic",
                new KafKaSerializationSchema<String>() {
                    ProducerRecord<byte[], byte[]> serialize(String str) {
                        return new ProducerRecord<byte[], byte[]>("dwd_traffic_err_log", str.getBytes());
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
            )
        );

    // 启动侧输出流数据写入kafka中
    pageDataStream
        .getSideOutput(startTag)
        .addSink(
            new FlinkKafkaProducer<String> (
                "default_topic",
                new KafKaSerializationSchema<String>() {
                    ProducerRecord<byte[], byte[]> serialize(String str) {
                        return new ProducerRecord<byte[], byte[]>("dwd_traffic_start_log", str.getBytes());
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
            )
        );

    // 曝光侧输出流数据写入kafka中
    pageDataStream
        .getSideOutput(displayTag)
        .addSink(
            new FlinkKafkaProducer<String> (
                "default_topic",
                new KafKaSerializationSchema<String>() {
                    ProducerRecord<byte[], byte[]> serialize(String str) {
                        return new ProducerRecord<byte[], byte[]>("dwd_traffic_display_log", str.getBytes());
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
            )
        );

    // 动作侧输出流数据写入kafka中
    pageDataStream
        .getSideOutput(actionTag)
        .addSink(
            new FlinkKafkaProducer<String> (
                "default_topic",
                new KafKaSerializationSchema<String>() {
                    ProducerRecord<byte[], byte[]> serialize(String str) {
                        return new ProducerRecord<byte[], byte[]>("dwd_traffic_action_log", str.getBytes());
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
            )
        );

    // 主流页面日志写入kafka中
    pageDataStream
        .addSink(
            new FlinkKafkaProducer<String> (
                "default_topic",
                new KafKaSerializationSchema<String>() {
                    ProducerRecord<byte[], byte[]> serialize(String str) {
                        return new ProducerRecord<byte[], byte[]>("dwd_traffic_page_log", str.getBytes());
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
            )
        );

// 将有向无环图提交到集群上去
env.execute();