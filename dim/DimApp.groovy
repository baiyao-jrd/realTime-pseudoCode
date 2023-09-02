DimApp

1. 本地环境准备
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironmnet()
1.1 设置并行度
env.setParallelism(4);

2. 检查点设置
2.1 开启检查点, 设置检查点时间是3000L, 设置检查点模式为精准一次
env.enableCheckpointing(3000L, CheckpointMode.EXACTLY_ONCE);

2.2 设置检查点超时时间为1min
env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);

2.3 设置两次检查点之间最小时间间隔
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);

2.4 设置取消任务后是否保留外部检查点
env.enableExternalizedcheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

2.5 设置重启策略
env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.Days(1L), Time.Minutes(1L)));
env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));

2.6 设置状态后端以及检查点保存路径
env.setStateBackend(new HashMapStateBackend());
env.getCheckpoint().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");
env.getCheckpointConfig().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");

2.7 设置hdfs的操作用户
System.setProperty("HADOOP_USER_NAME", baiyao);

3. 从kafka读取数据
Properties props = new Properties();
3.1 kafka集群地址
props.setProperty("bootstrap.servers", "zoo1:9092,zoo2:9092,zoo3:9092");
3.2 消费者组
props.setProperty("group.id", "dim_app_group");

topic = "topic_db";

FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>(
    // FlinkKafkaConsumer消费的主题
    topic,
    // 从kafka读取的数据需要反序列化
    new KafkaDeserializationschema<String>() {
        // 程序会一直运行, 永远没有最后一条数据, 返回false
        isEndOfStream return false;
        
        deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
            // 由于new String()的参数列表中传入的参数不能为null, 否则会报错. 
            // 所以, 需要判断一下consumerRecord.value是否为空, 为空的话就返回new String(consumerRecord.value()),
            // 否则直接返回null
            if (consumerRecord.value() != null)
                return new String(consumerRecord.value());
            else 
                return null;
        }

        // 返回反序列化之后数据类型String
        getProducedType()
            return TypeInformation.of(String.class)
    },
    props
)

env
    // 将FlinkKafkaConsumer<String>封装为流
    .addSource(flinkKafkaConsumer)
    // 静态导入语法, 将kafka读取过来的json字符串转化为JSONObject类型
    .map(JSON::parseObject)
    // 数据简单清洗: 能转化为jsonObj的数据向下传、过滤掉maxwell全量导入数据的时候开始和结束数据
    .filter(
        new FilterFunction<JSONObject>() {
            Boolean filter(JSONObject jsonObj) {
                try {
                    // 如果"data"数据是完整json, 返回true, 也就是向下传
                    jsonObj.getJSONObject("data");

                    String type = jsonObj.getString("type");

                    // 如果"data"是完整json, 同时还要注意maxwell初始bootstrap全量同步的时候有两条消息需要被过滤掉, 那么就返回false
                    if ("bootstrap-start".equals(type) || "bootstrap-complete".equals(type)) {
                        return false;
                    } 

                    return true;
                } catch(Exception e) {
                    // 不为完整字符串就返回false, 直接过滤掉
                    return false;
                }
            }
        }
    );

-> filterDataStream
// 使用flinkCDC读取Mysql表中配置数据
MySqlSource<String> mysqlSource = MySqlSource
    .<String>Builder()
    .hostname()
    .port()
    .databaseList()
    .tableList()
    .username()
    .password()
    .deserializer(new JsonDebeziumDeserializationSchema())
    .startupOptions(StartOptions.initial())
    .build();

-> MysqlDataStreamSource

// flinkCDC读取的配置数据, 读过来之后需要转换为实体类进行传递
TableProcess {
    String sourceTable;
    String sinkTable;
    String sinkColumns;
    String sinkPK;
    String sinkExtended;
}

// flinkCDC读取的配置数据需要被封装为流, 不指定水位线
env.fromSource(
    mysqlSource,
    WaterMarkStrategy.noWatermarks(),
    "MySqlSource"
)

// 配置数据需要被广播到每一个并行实例上, 用MapState存储配置数据, key为源表表名, value为实体类
MysqlDataStreamSource.broadcast(
    mapStateDescriptor = new MapStateDescriptor<String, TableProcess>() {
        "mapStateDescriptor",
        String.class,
        TableProcess.class
    }
)

-> broadcastStream

// 主流和配置流需要通过connect算子进行合并
filterDataStream.connect(
    broadcastStream
)
.process(
    BroadcastProcessFunction<JSONObject, String, JSONObject> {
        // 在open生命周期开始的时候获取德鲁伊数据源, 这个主要是为了处理配置流的时候连接phoenix, 用与提前建表.
        // 为的是防止维度表变更后, 配置表也变更了, 那么就需要在往phoenix表插入数据之前提前建好表
        open() {
            druidDataSource = DruidUtil.createDataSource();
        }

        // 对主流数据, 也就是从kafka读过来的数据进行处理
        processElement<jsonObj jsonObj, ReadOnlyContext context, Collector<JSONObject> out> {
            // 从算子状态MapState中拿变更数据对应的配置数据
            TableProcess tableProcess = readOnlyContext.getBroadcastState(mapStateDescriptor).get(jsonObj.getString("table"));

            // 能拿到说明是配置流数据
            if (tableProcess != null) {
                // 拿到变更数据
                JSONObject dataJsonObj = jsonObj.getJOSNObject("data");

                // 拿到配置数据的sinkColumns
                String sinkColumns = tableProcess.getSinkColumns();

                // 将sinkColumns转换为集合
                String[] column = sinkColumns.split(",");
                List<String> columnList = Arrays.asList(column);
                
                // 然后判断主流数据中某一列是否在配置表中有, 没有的话就从原来的jsonObj里面删除掉
                Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
                entrySet.removeIf(entry -> !columnList.contains(entry.getKey));

                // 向下游传递数据之前先将type以及sink_table也放入datajson中供后面使用
                dataJsonObj.put("type", jsonObj.getString("type"));
                dataJsonObj.put("sink_table", tabeProcess.getSinkTable());

                // 向下游传递data
                out.collect(dataJsonObj);
            }            
        }

        // 处理配置流数据
        processBroadcastElement<String jsonStr, Context context, Collector<JSONObject> out> {
            JSONObject jsonObj = jsonStr.parseObject("jsonStr");

            // 需要拿到配置流中op操作, 因为d与cru两种操作类型不同, 对状态进行的操作也不一样
            String operation = jsonObj.getString("op");

            // 拿到算子状态, 为删除以及cru更改操作做准备
            BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);

            if (operation.equals("d")) {
                // 删除的话, 通过将配置流中的before封装为实体类对象, 拿到被删除的表名, 使用remove将算子状态中的keyValue信息给删除了

                TableProcess tableProcess = jsonObj.getObject("before", TableProcess);

                broadcastState.remove(tableProcess.getSourceTable());

            } else {
                // c, r, u
                // 如果是创建、读或者更新的话, 需要把状态中的数据拿到之后进行更新

                // 将after封装为实体类, 为更新广播状态以及后面拼接建表语句做准备
                TableProcess tableProcess = jsonObj.getObj("after", TableProcess);
                // 将读取到的配置数据放入算子广播状态中
                broadcastState.put(tableProcess.getSourceTable(), tableProcess);

                // 需要通过实体类对象拿到目的表表名, 表字段, 表主键, 建表语句扩展来为后面拼接phoenixSql建表语句作准备
                String sinkTable = tableProcess.getSinkTable();
                String sinkColumns = tableProcess.getSinkColumns();
                String sinkPrimaryKey = tableProcess.getSinkPrimaryKey();
                String sinkExtended = tableProcess.getsinkExtended();

                // 拼接建表语句
                // 为防止建表扩展以及主键为空导致拼接建表语句报错, 提前赋好默认值
                if (sinkExtended == null) sinkExtended = "";
                if (sinkPrimaryKey == null) sinkPrimaryKey = "id";

                // 使用StringBuilder进行建表语句的拼接, 因为拼接字段的时候需要用到循环, 相对于String更好一些
                StringBuilder phoenixSql = new StringBuilder("create table if not exists " + SCHEMA + "." + sinkTable + " (")

                String[] column = sinkColumns.split(",");

                for(int i = 0, i < column.length, i++) {
                    // 是主键就加primary key, 不是的话就不加
                    if (column[i].equals(sinkPrimaryKey)) {
                        phoenixSql.append(column[i] + " varchar primary key")
                    } else {
                        phoenixSql.append(column[i] + " varchar")
                    }

                    // 非最后一个字段的话就加上逗号
                    if(i < column.length - 1) {
                        phoenixSql.append(",")
                    }
                }

                // 建表扩展不要忘了
                phoenixSql.append(") " + sinkExtended);

                // 使用德鲁伊数据源获取连接、获取操作对象、执行sql语句
                try {
                    Connection connection = druidDataSource.getConnection();
                    PreparedStatement preparedStatement = connection.prepareStatement(phoenixSql.toString());

                    preparedStatement.execute();
                } catch(Exception e) {
                    e.printStackTrace();
                } finally {
                    // 最后在finally中别忘了得关闭connection和preparedstatement
                    if (connection != null) {
                        try {
                            connection.close;
                        } catch(Exception e) {
                            e.printStackTrace;
                        }
                    }

                    if (preparedStatement != null) {
                        try {
                            preparedStatement.close;
                        } catch(Exception e) {
                            e.printStackTrace;
                        }
                    }
                }

                // 将读取到的配置流数据放到状态中给存储起来: key为源表名, value为配置表中的一行数据信息
                broadcastState.put(tableProcess.getSourceTable(), tableProcess);
            }
        }
    }
)
// 向phoenix中写数据
.addSink(
    // 通过富函数在open方法中获取德鲁伊连接池
    new RichSinkFunction<JSONObj>() {
        private DruidDataSource druidDataSource;

        open() {
            druidDataSource = DruidUtil.createDataSource();
        }

        // 
        invoke(JSONObject jsonObj, Context context) {
            // 获取目的表名以及操作类型为拼接upsert语句以及后面删除redis缓存数据作准备
            String sinkTable = jsonObj.getString("sink_table");
            String type = jsonObj.getString("type");

            jsonObj.remove("sink_table");
            jsonObj.remove("type");

            // 拼接建表语句, 通过upsert来保证精准一次
            String phoenixSql = "upsert into " + SCHEMA + "." + sinkTable + " (" + StringUtils.join(jsonObj.keySet(), ",") + ") " + "values('" + StringUtils.join(jsonObj.values(), "','") + "')"

            // 获取连接、获取操作对象、执行sql语句、释放connection以及preparedStatement资源
            try {
                Connnection connection = druidDataSource.getConnection;
                PreparedStatement preparedStatement = connection.prepareStatement(phoenixSql);
                preparedStatement.execute();
            } catch() {
                
            } finally {
                connection.close();
                preparedStatement.close();
            }
        }
    }
)

// 通过类名调用静态方法创建德鲁伊连接池, 使用懒汉式双重判断对象锁创建的德鲁伊连接池
DruidUtil {
    private static DruidDataSource druidDataSource;

    static DruidDataSource createDataSource() {
        
        if (druidDataSource != null) {
            Sychronized(DruidUtil.class) {
                if (druidDataSource == null) {
                    druidDataSource = new DruidDataSource();

                    // 连接的驱动类名以及连接的路径
                    druidDataSource.DriverClassName("org.apache.phoenix.jdbc.phoenixDriver");
                    druidDataSource.Url("jdbc:phoenix:zoo1,zoo2,zoo3:2181");

                    // 1. 初始化连接数量
                    druidDataSource.setInitialSize(5);
                    // 2. 最大连接数
                    druidDataSource.setMaxActive(20);
                    // 3. 空闲连接数
                    druidDataSource.setMinIdle(5);
                    // 4. 没有空闲连接时, 连接等待时长
                    druidDataSource.setMaxWait(-1);
                    // 5. 连接空闲多久进行回收
                    // 6. 连接回收器多久回收一次
                }
            }
        }

        return druidDataSource;
    }
}


4. 提交任务执行
env.execute();