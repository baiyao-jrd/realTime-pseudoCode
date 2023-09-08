DwsTradeSkuOrderWindow

-> 交易域sku粒度下单聚合统计

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
            // props.setProperty("group.id", "dws_trade_sku_order_group");

            props
        }
    )
    // 将String类型数据转换为JSONObject类型
    .map(JSON::parseObject)
    // {
    //     "create_time": "2022-09-01 10:05:28",
    //     "sku_num": "2",
    //     "activity_rule_id": "2",
    //     "split_original_amount": "16394.0000",
    //     "sku_id": "11",
    //     "date_id": "2022-09-01",
    //     "source_type_name": "智能推荐",
    //     "user_id": "65",
    //     "province_id": "1",
    //     "source_type_code": "2403",
    //     "row_op_ts": "2022-09-14 02:05:30.085Z",
    //     "activity_id": "1",
    //     "sku_name": "Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机",
    //     "id": "239",
    //     "order_id": "109",
    //     "split_activity_amount": "1200.0",
    //     "split_total_amount": "15194.0",
    //     "ts": "1663121128"
    // }
    
    // 按照唯一键 order_detail_id 进行分组
    // 第一次keyby来进行去重
    .keyBy(
        r -> r.getString("id")
    )
    // flink状态编程 + 定时器去重
    // 下单的订单明细事实表里面的数据来源于订单预处理表, 由于订单预处理表的主表明细表与订单表之间是内连接, 
    // 两表内连接之后与订单明细活动以及订单明细优惠券使用的都是左外连接, 那么就会出现3条数据
    // 左表先来 -> +I 左null
    // 右表数据 -> +D 左null
    //         -> +I 左右

    // 上面是动态表数据, 接着要往kafka里面写会有3条数据 1. 左null 2. null 3. 左右
    // 使用kafka连接器读数据会自动把null数据过滤掉, 但是1,3仍会重复
    // 这个重复对及算独立访客没有影响, 但是对计算订单总额来说, 数据会重复
    // 加的row_op_ts字段就是用来去重的

    // 订单预处理表的结果是使用upsert-kafka写的, 其中建表语句中的primary key(id) not enforced指定的就是明细id
    // 所以同一个明细id就能进入同一个分区中来保证先后顺序, 显然不完整的数据先来, 完整的数据会后来
    .process(
        // key, in, out
        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            // 状态的声明
            private ValueState<JSONObject> lastJsonObjState;

            open() {
                ValueStateDescriptor<JSONObject> valueStateDescriptor =
                    new ValueStateDescriptor<JSONObject>(
                        "lastJsonObjState",
                        JSONObject.class
                    );

                // 状态的初始化
                lastJsonObjState = getRuntimeContext()
                    .getState(
                        valueStateDescriptor
                    );
            }

            // 对流中数据一条条处理
            processElement(JSONObject jsonObj, Context context, Collector<JSONObject> out) {
                JSONObject lastJsonObj = lastJsonObjState.value();

                if (lastJsonObj == null) {
                    lastJsonObjState.update(jsonObj);
                    //注册定时器
                    long currentProcessingTime = context.timerService().currentProcessingTime();

                    // 两条重复数据间隔不可能大于5秒, 如果有, 说明程序链路传输延迟过大
                    context.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
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
                        lastJsonObjState.update(jsonObj);
                    }
                }
            }

            // 
            onTimer(long timestamp, OnTimerContext context, Collector<JSONObject> out) {
                JSONObject jsonObj = lastJsonObjState.value();

                if (jsonObj != null) {
                    out.collect(jsonObj);
                }
                
                // 数据往下传递之后, 状态里面数据就没必要保存了
                lastJsonObjState.clear();
            }
        }
    )
    // 类型转换 -> jsonObj转换为实体类对象 -> 方便后面作聚合

    /*--------------------------------------------- TradeSkuOrderBean ----------------------------------------------------*/

    @Data
    @AllArgsConstructor
    @Builder
    TradeSkuOrderBean {
        String stt;                 -> 窗口起始时间
        String edt;                 -> 窗口结束时间

        String trademarkId;         -> 品牌 ID
        String trademarkName;       -> 品牌名称
        String category1Id;         -> 一级品类 ID
        String category1Name;       -> 一级品类名称
        String category2Id;         -> 二级品类 ID
        String category2Name;       -> 二级品类名称
        String category3Id;         -> 三级品类 ID
        String category3Name;       -> 三级品类名称

        // 由于其用来对订单计数, 所以没必要往clickhouse里面保存, 那么这里就加上注解
        @TransientSink
        Set<String> orderIdSet;     -> 订单 ID

        // 用来计算独立用户数的, 也不用往clickhouse里面保存
        @TransientSink
        String userId;              -> 用户 ID

        String skuId;               -> sku_id
        String skuName;             -> sku 名称
        String spuId;               -> spu_id
        String spuName;             -> spu 名称

        Long orderUuCount;          -> 独立用户数
        Long orderCount;            -> 下单次数
        Double originalAmount;      -> 原始金额
        Double activityAmount;      -> 活动减免金额
        Double couponAmount;        -> 优惠券减免金额
        Double orderAmount;         -> 下单金额

        Long ts;                    -> 时间戳
    }

    /*--------------------------------------------- TradeSkuOrderBean ----------------------------------------------------*/

    .map(
        new MapFunction<JSONObject, TradeSkuOrderBean>() {
            // {
            //     "create_time": "2022-09-01 10:05:28",
            //     "sku_num": "2",
            //     "activity_rule_id": "2",
            //     "split_original_amount": "16394.0000",
            //     "sku_id": "11",
            //     "date_id": "2022-09-01",
            //     "source_type_name": "智能推荐",
            //     "user_id": "65",
            //     "province_id": "1",
            //     "source_type_code": "2403",
            //     "row_op_ts": "2022-09-14 02:05:30.085Z",
            //     "activity_id": "1",
            //     "sku_name": "Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机",
            //     "id": "239",
            //     "order_id": "109",
            //     "split_activity_amount": "1200.0",
            //     "split_total_amount": "15194.0",
            //     "ts": "1663121128"
            // }
            TradeSkuOrderBean map(JSONObject jsonObj) {
                String orderId = jsonObj.getString("order_id");
                String userId = jsonObj.getString("user_id");
                String skuId = jsonObj.getString("sku_id");

                Double splitOriginalAmount = jsonObj.getDouble("split_original_amount");
                Double splitActivityAmount = jsonObj.getDouble("split_activity_amount");
                Double splitCouponAmount = jsonObj.getDouble("split_coupon_amount");
                Double splitTotalAmount = jsonObj.getDouble("split_total_amount");

                // "ts": "1663121128" 秒级时间戳需要转换为毫秒级
                Long ts = jsonObj.getLong("ts") * 1000L;

                TradeSkuOrderBean orderBean = TradeSkuOrderBean
                    .builder()
                    // orderCount计数用: 不能说每来一条数据就订单数加一, 明细为主表, 订单表中某一个id会对应于明细表多条数据
                    .orderIdSet(
                        // set集合有去重能力, 每条数据的订单id放入set集合里面
                        new HashSet<String>(
                            Collections
                                .singleton(orderId)
                        )
                    )
                    .skuId(skuId)
                    .userId(userId)
                    .orderUuCount(0L)
                    .originalAmount(splitOriginalAmount)
                    // 并不是每一个商品都参与了优惠券或者活动优惠, 所以数据有可能等于空, 两者在这里要单独处理
                    .activityAmount(splitActivityAmount == null ? 0.0 : splitActivityAmount)
                    .couponAmount(splitCouponAmount == null ? 0.0 : splitCouponAmount)
                    .orderAmount(splitTotalAmount)
                    .ts(ts)
                    .build();

                return orderBean;
            }
        }
    )
    // 接下来要计算各窗口的独立用户数
    // 按照用户id进行分组
    .keyBy(TradeSkuOrderBean::getUserId)
    // 使用Flink的状态编程，判断下单独立用户
    .process(
        // key, in, out
        new KeyedProcessFunction<String, TradeSkuOrderBean, TradeSkuOrderBean>() {
            // 状态的声明
            private ValueState<String> lastOrderDateState;

            open() {
                ValueStateDescriptor<String> valueStateDescriptor =
                    new ValueStateDescriptor<String>(
                        // 状态名称
                        "lastOrderDateState",
                        // 当前状态中存的数据类型
                        String.class
                    );
                
                // 统计的当天的独立访客, 状态没必要一直保存, 所以这里状态只保存一天
                valueStateDescriptor.enableTimeToLive(
                    StateTtlConfig
                        // 通过构造者设计模式创建对象
                        .newBuilder(Time.days(1))
                        .build()
                );
                
                // 状态的初始化
                lastOrderDateState = getRuntimeContext()
                    .getState(
                        valueStateDescriptor
                    );
            }

            // 对流中数据一条条处理
            processElement(TradeSkuOrderBean orderBean, Context context, Collector<TradeSkuOrderBean> out) {
                String lastOrderDate = lastOrderDateState.value();

                String curOrderDate = DateTimeFormatter
                    .ofPattern("yyyy-MM-dd")
                    .format(
                        LocalDateTime
                            .ofInstant(
                                // 需要的就是13位时间戳
                                new Date(orderBean.getTs()).toInstant(),
                                ZoneId.systemDefault()
                            )
                    );

                if (StringUtils.isEmpty(lastOrderDate) || !lastOrderDate.equals(curOrderDate)) {
                    orderBean.setOrderUuCount(1L);
                    lastOrderDateState.update(curOrderDate);
                }

                out.collect(orderBean);
            }
        }
    )
    // 指定水位线以及提取事件时间字段
    .assignTimestampsAndWatermarks(
        WaterMarkStrategy
            .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner(
                new SerializationTimestampAssigner<TradeSkuOrderBean>() {
                    long extractTimestamp(TradeSkuOrderBean orderBean, long timestamp) {
                        return orderBean.getTs();
                    }
                }
            )
    )
    // 按照sku维度进行分组
    .keyBy(TradeSkuOrderBean::getSkuId)
    // 开窗
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    // 聚合
    .reduce(
        new ReduceFunction<TradeSkuOrderBean>() {
            TradeSkuOrderBean reduce(TradeSkuOrderBean acc, TradeSkuOrderBean in) {

                // 将两个set集合作合并
                acc.getOrderIdSet().addAll(in.getOrderIdSet());
                acc.setOrderUuCount(acc.getOrderUuCount() + in.getOrderUuCount());
                acc.setOriginalAmount(acc.getOriginalAmount() + in.getOriginalAmount());
                acc.setActivityAmount(acc.getActivityAmount() + in.getActivityAmount());
                acc.setCouponAmount(acc.getCouponAmount() + in.getCouponAmount());
                acc.setOrderAmount(acc.getOrderAmount() + in.getOrderAmount());

                return acc;
            }
        },
        // in, out, key, window
        new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
            apply(
                String key,
                TimeWindow window, 
                // 这里窗口聚合之后只有一个结果, 也就是只有一个元素
                Iterable<TradeSkuOrderBean> input, 
                Collector<TradeSkuOrderBean> out
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

                for (TradeSkuOrderBean orderBean : input) {
                    orderBean.setStt(start);
                    orderBean.setEdt(end);
                    orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                    orderBean.setTs(System.currentTimeMillis());

                    out.collect(orderBean);
                }
            }
        }
    );
    // TradeSkuOrderBean (
    //     stt=2022-09-15 14:41:10, 
    //     edt=2022-09-15 14:41:20, 
    //     trademarkId=null, 
    //     trademarkName=null, 
    //     category1Id=null, 
    //     category1Name=null, 
    //     category2Id=null, 
    //     category2Name=null, 
    //     category3Id=null, 
    //     category3Name=null, 
    //     orderIdSet=[129], 
    //     userId=34, 
    //     skuId=12, 
    //     skuName=null, 
    //     spuId=null, 
    //     spuName=null, 
    //     orderUuCount=1, 
    //     orderCount=1, 
    //     originalAmount=18394.0, 
    //     activityAmount=1200.0, 
    //     couponAmount=0.0, 
    //     orderAmount=17194.0, 
    //     ts=1663224110149
    // )
    
    // 和sku维度进行关联
    // 将异步I/O操作应用于DataStream作为DataStream的一次转换操作
    withSkuInfoDS = AsyncDataStream.unorderedWait(
        reduceDS,
        //实现分发请求的AsyncFunction
        new DimAsyncFunction<TradeSkuOrderBean>("dim_sku_info") {
            join(TradeSkuOrderBean orderBean, JSONObject dimInfoJsonObj) {
                orderBean.setSkuName(dimInfoJsonObj.getString("SKU_NAME"));
                orderBean.setTrademarkId(dimInfoJsonObj.getString("TM_ID"));
                orderBean.setCategory3Id(dimInfoJsonObj.getString("CATEGORY3_ID"));
                orderBean.setSpuId(dimInfoJsonObj.getString("SPU_ID"));
            }

            String getKey(TradeSkuOrderBean orderBean) {
                return orderBean.getSkuId();
            }
        },
        60,
        TimeUnit.SECONDS
    );

    /*--------------------------------------------- 模板方法设计模式 ----------------------------------------------------*/

    * 模板方法设计模式
    *      在父类中定义完成某一个功能的核心算法的骨架(步骤),将具体的实现延迟到子类中去完成。
    *      在不改变父类核心算法骨架的前提下，每一个子类都可以有自己不同的实现。

    abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {

        ExecutorService executorService;
        DruidDataSource dataSource;
        String tableName;

        DimAsyncFunction(String tableName) { this.tableName = tableName; }

        open(Configuration parameters) {
            executorService = ThreadPoolUtil.getInstance();
            dataSource = DruidDSUtil.createDataSource();
        }

        asyncInvoke(T obj, ResultFuture<T> resultFuture) {
            //开启多线程，发送异步请求
            executorService.submit(
                new Runnable() {
                    run() {
                        Connection conn = null;
                        try {
                            //根据流中的对象获取要关联的维度的主键
                            String key = getKey(obj);
                            conn = dataSource.getConnection();
                            //根据维度的主键获取维度对象
                            JSONObject dimInfoJsonObj = DimUtil.getDimInfo(conn, tableName, key);
                            if(dimInfoJsonObj != null){
                                //将维度对象的属性补充到流中的对象上
                                join(obj,dimInfoJsonObj);
                            }
                            //获取数据库交互的结果并发送给ResultFuture的回调函数
                            resultFuture.complete(Collections.singleton(obj));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }finally {
                            if(conn != null){
                                try {
                                    conn.close();
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                    }
                }
            );
        }
    }

    /*--------------------------------------------- 模板方法设计模式 ----------------------------------------------------*/

    /*----------------------------------------- DruidDSUtil.createDataSource() ------------------------------------------------*/

        DruidDSUtil {
            static DruidDataSource druidDataSource;

            static DruidDataSource createDataSource() {
                if(druidDataSource == null){
                    synchronized (DruidDSUtil.class){
                        if(druidDataSource == null){
                            druidDataSource = new DruidDataSource();    -> 创建连接池
                            druidDataSource.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver"); -> 设置驱动全类名
                            druidDataSource.setUrl("jdbc:phoenix:hadoop202,hadoop203,hadoop204:2181"); -> 设置连接 url
                            druidDataSource.setInitialSize(5);  -> 设置初始化连接池时池中连接的数量
                            druidDataSource.setMaxActive(20);   -> 设置同时活跃的最大连接数
                            // 设置空闲时的最小连接数，必须介于 0 和最大连接数之间，默认为 0
                            druidDataSource.setMinIdle(5);
                            // 设置没有空余连接时的等待时间，超时抛出异常，-1 表示一直等待
                            druidDataSource.setMaxWait(-1);
                            // 验证连接是否可用使用的 SQL 语句
                            druidDataSource.setValidationQuery("select 1");
                            // 指明连接是否被空闲连接回收器（如果有）进行检验，如果检测失败，则连接将被从池中去除
                            // 注意，默认值为 true，如果没有设置 validationQuery，则报错
                            // testWhileIdle is true, validationQuery not set
                            druidDataSource.setTestWhileIdle(true);
                            // 借出连接时，是否测试，设置为 false，不测试，否则很影响性能
                            druidDataSource.setTestOnBorrow(false);
                            // 归还连接时，是否测试
                            druidDataSource.setTestOnReturn(false);
                            // 设置空闲连接回收器每隔 30s 运行一次
                            druidDataSource.setTimeBetweenEvictionRunsMillis(30 * 1000L);
                            // 设置池中连接空闲 30min 被回收，默认值即为 30 min
                            druidDataSource.setMinEvictableIdleTimeMillis(30 * 60 * 1000L);
                        }
                    }
                }
                return druidDataSource;
            }
        }

    /*----------------------------------------- DruidDSUtil.createDataSource() ------------------------------------------------*/

    /*----------------------------------------- ThreadPoolUtil.getInstance() ------------------------------------------------*/

        * 获取线程池的工具类
        * 双重校验锁解决单例设计模式懒汉式线程安全问题

        ThreadPoolUtil {
            static volatile ThreadPoolExecutor poolExecutor;

            static ThreadPoolExecutor getInstance(){
                if(poolExecutor == null){
                    synchronized(ThreadPoolUtil.class){
                        if(poolExecutor == null){
                            poolExecutor = new ThreadPoolExecutor(
                                4,
                                20,
                                300, 
                                TimeUnit.SECONDS,
                                new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                        }
                    }
                }

                return poolExecutor;
            }
        }

    /*----------------------------------------- ThreadPoolUtil.getInstance() ------------------------------------------------*/

    /*----------------------------------------- 旁路缓存 ------------------------------------------------*/

    DimUtil {

        static JSONObject getDimInfo(Connection conn, String tableName, String id) {
            return getDimInfo(conn, tableName, Tuple2.of("id", id));
        }

        /**
        * 选型：
        * Redis √
        * 性能也不错、维护性好
        * 状态
        * 性能好、维护性差
        * Redis使用分析：
        * 类型：     String
        * key：     dim:维度表表名:主键1_主键2
        * TTL:      1day
        * 注意： 如果业务数据库维度表发生了变化，将缓存中的维度删除
        */
    
        // 经优化的旁路缓存策略: 

        //      来一条聚合数据之后, 没必要来phoenix表里面进行查询, 直接来redis缓存里面查一下, 看看有没有这个维度
        //      缓存命中: 缓存里面有数据的话, 就直接把维度数据拿过来进行关联
        //      未命中: 发送请求到phoenix查询维度数据, 与聚合数据关联后并将此数据再放入redis缓存中以便于下次查询

        static JSONObject getDimInfo(Connection conn, String tableName, Tuple2<String, String>... columnNameAndValues) {
            //拼接从Redis中查询维度的key
            StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");
            StringBuilder selectSql = new StringBuilder("select * from " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + " where ");
            for (int i = 0; i < columnNameAndValues.length; i++) {
                Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
                String columnName = columnNameAndValue.f0;
                String columnValue = columnNameAndValue.f1;
                redisKey.append(columnValue);
                selectSql.append(columnName + "='" + columnValue + "'");
                if (i < columnNameAndValues.length - 1) {
                    redisKey.append("_");
                    selectSql.append(" and ");
                }
            }

            Jedis jedis = null;
            String dimInfoStr = null;
            JSONObject dimInfoJsonObj = null;

            try {
                jedis = RedisUtil.getJedis();
                dimInfoStr = jedis.get(redisKey.toString());
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("~~从Redis中查询维度数据发生了异常~~");
            }

            if (StringUtils.isNotEmpty(dimInfoStr)) {
                // 说明从redis中找到了对应的维度数据(缓存命中)
                dimInfoJsonObj = JSON.parseObject(dimInfoStr);
            } else {
                // 说明从redis中没有找到对应的维度数据,发送请求到phoenix表中查询维度
                System.out.println("从phoenix表中查询维度的SQL:" + selectSql);
                // 查询维度的时候，底层还是调用的是PhoenixUtil中的queryList
                // 方便处理, 对于查询出来的维度数据都封装成了json对象
                List<JSONObject> dimInfoJsonList = PhoenixUtil.queryList(conn, selectSql.toString(), JSONObject.class);
                if (dimInfoJsonList != null && dimInfoJsonList.size() > 0) {
                    //注意:因为我们是根据维度的主键的去查询维度数据，所以如果集合不为空，那么返回的维度只会有一条
                    dimInfoJsonObj = dimInfoJsonList.get(0);
                    //将查询的结果放到redis中进行缓存,并指定失效时间
                    if (jedis != null) {
                        jedis.setex(redisKey.toString(), 3600 * 24, dimInfoJsonObj.toJSONString());
                    }
                } else {
                    System.out.println("~~在phoenix表中没有查到对应的维度信息~~");
                }
            }

            //关闭连接
            if (jedis != null) {
                System.out.println("~~关闭Jedis客户端~~");
                jedis.close();
            }

            return dimInfoJsonObj;
        }

        //查询维度数据 没有任何优化

        //      未经优化的维度关联: 来一条聚合数据就会到phoenix中查询一次维度数据, 交互过于频繁
        static JSONObject getDimInfoNoCache(Connection conn, String tableName, Tuple2<String, String>... columnNameAndValues) {
            // GmallConfig.PHOENIX_SCHEMA : GMALL0321_SCHEMA
            StringBuilder selectSql = new StringBuilder("select * from " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + " where ");
            for (int i = 0; i < columnNameAndValues.length; i++) {
                Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
                String columnName = columnNameAndValue.f0;
                String columnValue = columnNameAndValue.f1;
                selectSql.append(columnName + "='" + columnValue + "'");
                if (i < columnNameAndValues.length - 1) {
                    selectSql.append(" and ");
                }
            }

            //查询维度的时候，底层还是调用的是PhoenixUtil中的queryList
            List<JSONObject> dimInfoJsonList = PhoenixUtil.queryList(conn, selectSql.toString(), JSONObject.class);
            JSONObject dimInfoJsonObj = null;
            if (dimInfoJsonList != null && dimInfoJsonList.size() > 0) {
                //注意:因为我们是根据维度的主键的去查询维度数据，所以如果集合不为空，那么返回的维度只会有一条
                dimInfoJsonObj = dimInfoJsonList.get(0);
            } else {
                System.out.println("~~在phoenix表中没有查到对应的维度信息~~");
            }

            return dimInfoJsonObj;
        }


        static void delCached(String tableName, String id) {
            String redisKey = "dim:" + tableName.toLowerCase() + ":" + id;
            //dim:维度表表名:主键1_主键2
            Jedis jedis = null;
            try {
                jedis = RedisUtil.getJedis();
                jedis.del(redisKey);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if(jedis != null){
                    System.out.println("~~从Redis中删除维度后，关闭Jedis客户端~~");
                    jedis.close();
                }
            }
        }
    }


    /*----------------------------------------- 旁路缓存 ------------------------------------------------*/

    /*----------------------------------------- PhoenixUtil.queryList ------------------------------------------------*/

    public class PhoenixUtil {
        //从phoenix数据库表中查询数据
        // 将查询出来的记录用一个实体类T封装, 由于有多条记录, 所以用List承接实体类集合
        // Connection 将连接对象传过来
        // sql 执行的查询语句也传过来
        // Class<T> clz -> 将查询的结果封装成什么样类型的对象 -> 传进来的是一个对象

        // 静态方法中使用泛型T, 就需要在方法返回值List<T>前面声明一下<T>
        public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz) {
            // 查询结果转换为实体类对象后的集合
            List<T> resList = new ArrayList<>();
            // 操作对象
            PreparedStatement ps = null;
            // 结果集
            ResultSet rs = null;

            try {
                //获取数据库操作对象
                ps = conn.prepareStatement(sql);
                //执行SQL查询语句
                rs = ps.executeQuery();

                // 处理结果集
                /*
                +-----+-------+------------+------------+-----------+-------------+
                | ID  | NAME  | REGION_ID  | AREA_CODE  | ISO_CODE  | ISO_3166_2  |
                +-----+-------+------------+------------+-----------+-------------+
                | 1   | 北京   | 1          | 110000     | CN-11     | CN-BJ       |
                | 10  | 福建   | 2          | 350000     | CN-35     | CN-FJ       |
                */

                // resultSet结果集拿到之后, 怎么处理? 
                ResultSetMetaData metaData = rs.getMetaData();
                while (rs.next()) {
                    // 定义一个对象，用于封装查询出来的每一条记录
                    // 由类获得类对象
                    T obj = clz.newInstance();

                    // 通过ResultSetMetaData拿到一共有多少列
                    // jdbc的列从1开始
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        // 对于拿到的第i条数据
                        +-----+-------+------------+------------+-----------+-------------+
                        | 1   | 北京   | 1          | 110000     | CN-11     | CN-BJ       |


                        +-----+-------+------------+------------+-----------+-------------+
                        | ID  | NAME  | REGION_ID  | AREA_CODE  | ISO_CODE  | ISO_3166_2  |
                        +-----+-------+------------+------------+-----------+-------------+
                        // 拿到第i条数据的第i列列名
                        String columnName = metaData.getColumnName(i);
                        // 拿到第i条数据的第i列列值
                        Object columnValue = rs.getObject(i);

                        // 不必通过反射进行属性赋值
                        // 列名列值给属性赋值, 使用工具类, 给obj对象的columnName属性赋值为columnValue
                        BeanUtils.setProperty(obj,columnName,columnValue);
                    }

                    // 把某条数据对应的实体类对象添加到集合中
                    resList.add(obj);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("从phoenix表中查询维度数据发生了异常");
            } finally {
                //释放结果集与操作对象资源
                if (rs != null) {
                    try { rs.close(); } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                if (ps != null) {
                    try { ps.close(); 
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
            return resList;
        }
    }

    /*----------------------------------------- PhoenixUtil.queryList ------------------------------------------------*/