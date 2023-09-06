DwdTrafficSourceKeywordPageViewWindow

-> 流量域来源关键词聚合统计

// 流环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 并行度
env.setParallelism(4);
// 表执行环境
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
// 注册自定义函数
tableEnv
    .createTemporarySystemFunction(
        "ik_analyze", KeywordUDTF.class
    );

/*------------------------------------------- KeywordUDTF ------------------------------------------------------*/

// 这里定义的是每一行(Row)中都有哪些列, 这里只有一列, 就是分好的关键词
@FunctionHint(output = @DataTypeHint("ROW<word string>"))
// 这里使用Row类型来封装最后分好的一个个单词
KeywordUDTF extends TableFunction<Row> {

    // 这里的eval方法不是实现, 而是重载, 专门用于实现1转多的方法
    eval(String text) {
        List<String> keywordList = new ArrayList<>();
        StringBuilder reader = new StringBuilder(text);

        // 设为true, 智能分词 -> 合并数词和量词而不是细粒度输出所有切分结果
        IKSegmenter ik = new IKSegmenter(reader, true);

        try {
            // /ˈlɛksiːm/ 莱克sei姆
            Lexeme lexeme = null;

            // ik是分好词了已经, 调用next().getLexemeText()得到所有分词的结果
            // 最后将所有分好的词放进ArrayList当中
            while((lexeme = ik.next()) != null) {
                keywordList.add(lexeme.getLexemeText());
            }
        } catch {
            e.printStackTrace();
        }

        for (String keyword : keyWordList) {
            collect(Row.of(keyword));
        }
    }
}

/*-------------------------------------------------------------------------------------------------*/

// 每5s开启一次检查点, 模式涉及barriar对齐是精准一次还是至少一次
env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
// 检查点超时时间为1min
env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
// 两次检查点之间的最小时间间隔
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
// job取消之后检查点(状态的副本)是否保留, 
// 生产环境下可能是每一分钟开启一次检查点, 那么在程序做升级的时候, 停止程序之前我需要手动保存检查点(保存点 -> 用于手动备份),
// 这里需要在你取消任务之后, 检查点或者保存点仍要进行保留, 否则程序升级完成之后重启, 检查点没了就毁了 
env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
// flink程序在异常的时候会尝试重启, 默认是Integer的最大值这么多次
// 次数、计数周期、3秒重启一次 -> 30天有3次机会, 过了30天会把机会重新清零
env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
// 设置状态后端
env.setStateBackend(new HashMapStateBackend());
env.getCheckpointConfig().setCheckpointStorage("hdfs://zoo1:8020/gmall/ck");
// 操作hadoop的用户
System.setProperty("HADOOP_USER_NAME", baiyao);

// 从kafka的dwd_traffic_page_log主题中读取数据并创建动态表, 指定Watermark以及提取事件时间字段
tableEnv
    .executeSql(
        "create table page_log (\n" +
        "    common map<string, string>,\n" +
        "    page map<string, string>,\n" +
             // 13位时间戳
        "    ts bigint,\n" +
             // 开窗提交时间需要水位线, 下面两行是flinkSql方式提取事件时间字段
             // 建表的时候指定水位线
             // 将bigint类型的ts转换为timestamp(3)类型, 这个类型是指定水位线必须使用的类型
             // to_timestamp(字符串类型), 需要使用from_unixtime(十位时间戳 -> 即表示秒)转换为yyyy-MM-dd HH:mm:ss格式的字符串
             
             // 把当前TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))结果作为表中一个字段rowtime
             // 10位的秒级时间戳
        "    rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),\n" +
         
             // 指定有界乱序时间戳, 乱序时间为3秒
        "    watermark for rowtime as rowtime - interval '3' second\n" +
        ") with (\n" +
        "    'connector' = 'kafka',\n" +
        "    'topic' = 'dwd_traffic_page_log',\n" +
        "    'properties.bootstrap.servers' = 'zoo1:9092',\n" +
        "    'properties.group.id' = 'dws_traffic_keyword_group',\n" +
        "    'scan.startup.mode' = 'group-offsets',\n" +
        "    'format' = 'json'\n" +
        ")"
    );

// 过滤出搜索行为
// 页面日志中并不是所有行为都是搜索行为, 得把搜索行为过滤一下
// 搜索条件: last_page_id = 'search', item = '口红', item_type = 'keyword'
// 按照关键词搜索(有的是按照品类搜索, 咱们这里筛选关键词搜索), 搜索内容是'口红'

Table searchTable = tableEnv
    .sqlQuery(
        "select\n" +
        "    page['item'] fullword,\n" +
        "    rowtime\n" +
        "from page_log\n" +
        "where page['last_page_id'] = 'search' and page['item_type'] = 'keyword'\n" +
        // 下面这个条件可加可不加, 一般网站你搜索框不填关键词, 它也有默认关键词
        "and page['item'] is not null"
    );

tableEnv.createTemporaryView(
    "search_table",
    searchTable
);

// 使用自定义函数对搜索内容进行分词, 并和原表的其他字段进行连接
Table splitTable = tableEnv
    .sqlQuery(
        "select\n" +
        "    keyword,\n" +
        "    rowtime\n" +
        // lateral table(ik_analyze(fullword) t(keyword))表函数执行的结果会和左表search_table的每一行进行连接
        // 其中table(ik_analyze(fullword)函数执行的结果作为临时表存储起来, 表名可以随便起, 这里使用t作表名
        // 函数返回的结果是ROW, ROW里面只有一列就是分词结果, 这里用keyword表示
        "from search_table, lateral table(ik_analyze(fullword) t(keyword)) "
    );

tableEnv.createTemporaryView(
    "split_table",
    splitTable
);

// 分组、开窗、聚合、计算
Table reduceTable = tableEnv
    .sqlQuery(
        "select\n" +
             // 拿窗口的开始时间
        "    date_format(tumble_start(row_time, interval '10' second), 'yyyy-MM-dd HH:mm:ss') stt,\n" +
             // 拿窗口的结束时间
        "    date_format(tumble_end(row_time, interval '10' second), 'yyyy-MM-dd HH:mm:ss') edt,\n" +
             // 关键词来源其实就是search
        "    'search' source,\n" +
             // 分词器分词后的 关键词
        "    keyword,\n" +
             // 关键词聚合后的统计个数
        "    count(*) keyword_count,\n" +
             // 给一个聚合操作执行的当前系统时间: 13位的毫秒级时间戳
             // unix_timestamp()得到秒级时间戳
        "    unix_timestamp() * 1000 ts\n" +
        "from split_table\n" +
        // flinkSql开窗方式, 将tumble(rowtime, interval '10' second)放在group by后面
        // 开窗需要指定事件时间, 事件时间就是split_table里面的rowtime, 窗口大小是10s
        "group by tumble(rowtime, interval '10' second), keyword"
    );

/*--------------------------------------------- keywordBean ----------------------------------------------------*/

KeywordBean {
    String stt, -> 窗口起始时间
    String edt, -> 窗口闭合时间
    String source, -> 关键词来源
    String keyword, -> 关键词
    Long keyword_count, -> 关键词出现频次
    Long ts -> 时间戳
}

/*-------------------------------------------------------------------------------------------------*/

/*--------------------------------------------- clickHouse建表语句 ----------------------------------------------------*/

drop table if exists dws_traffic_source_keyword_page_view_window;

// 当前表中字段得和流中的属性对应起来
create table if not exists dws_traffic_source_keyword_page_view_window (
    stt DateTime,
    edt DateTime,
    source String,
    keyword String,
    // UInt64 -> 无符号整数64位
    keyword_count UInt64,
    ts UInt64
  // 副本去重引擎, 为什么不用指定主键primary key去重? 
                  clickhouse的MergeTree中有primary key, 但是和传统的不一样, 他这里不具备唯一约束
                  他在这里的作用是只会创建一级索引, 这个索引是稀疏索引, mysql的索引是密集索引(把每一条数据都做一个索引)
                  这里的一级索引代表一个范围, 你拿到索引以后到一个范围里面去查, 它并不具有唯一约束作用
) engine = ReplicatedReplacingMergeTree(ts)
  // 将窗口开始时间提取年月日来做分区
  partition by toYYYYMMDD(stt)
  // engine与partition by是可选的, 而这个order by是必选的, 他这里有两个作用
  // 1. 排序
  // 2. ReplicatedReplacingMergeTree按照这个order by中指定的字段(stt, edt, source, keyword)进行去重

  // 那么ReplicatedReplacingMergeTree(ts)中的ts字段有什么用呢?
  // 假设两条数据重复了, 干掉哪一条呢? 在这里ts控制的是版本, 要把版本大(ts大的)的给保留
  order by (stt, edt, source, keyword);  

/*-------------------------------------------------------------------------------------------------*/

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

tableEnv
    // 将动态表转换为流
    // 转换为流的两种: 1. toAppendStream -> 不涉及修改
    //                2. toRestractStream -> 回撤流, 也就是设计修改
    // 转换的是一个个窗口的数据, 下一个数据就是另一个窗口的数据了, 不涉及回撤
    
    // 将表对象中的一行行记录, 用实体类对象keywordBean进行封装
    // 这里要注意实体类属性名称要和select字段的别名一一对应
    .toAppendStream(reduceTable, keywordBean.class)
    // 将流中数据写到clickhouse表中去

    // 需要考虑往外部系统写入的时候的一致性问题
    // kafka -> 事务 -> 两阶段提交
    // phoenix -> upsert
    // clickhouse -> replacingMergeTree -> 因为不能保证马上去重, 它是merge的时候才进行去重的, 
    // 所以为了保证每次查询的数据一致性, 需要加关键字final来保证去重操作

    // 这里用了反射机制来往clickhouse中写数据
    .addSink(
        MyClickhouseUtil.getSinkFunction(
            "insert into dws_traffic_source_keyword_page_view_window values (?, ?, ?, ?, ?, ?)"
        )
    );

env.execute();