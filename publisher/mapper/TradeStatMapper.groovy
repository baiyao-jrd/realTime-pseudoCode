// 总交易额mapper层代码实现

interface TradeStatMapper {
    // 接口里面提供一个方法来获取某一天的总交易额
    BigDecimal selectGMV(Integer date);
}

// 举例, 一般情况下
MyClass implements TradeStatMapper {
    BigDecimal selectGMV(Integer date) {
        String sql =
            "select\n" +
            "    sum(order_amount) order_amount\n" +
            "from dws_trade_province_order_window\n" +
            "where toYYYYMMDD(stt) = 20220917";

        try {
            // 注册驱动
            Class.forName("");
            // 获取连接
            Connection conn = DriverManager.getConnection("");
            // 获取数据库操作对象
            PreparedStatement ps = conn.prepareStatment(sql);
            // 执行sql语句
            ResultSet rs = ps.executeQuery();
            // 处理结果集
            rs.next() -> 获取查询结果
            rs.getObject(1) -> 拿到第一列数据就是总交易额
        } catch (Exception e) {
            
        } finally {
            // 释放资源
        }

        return null;
    }
}

// 注意上面的套路是通用的, jdbc这一套被Mybatis框架封装了
@Insert("")
@Delete("")
@Update("")
@Select("")

interface TradeStatMapper {
    //获取某一天总交易额
    // #{date} -> 不能写死, 要作为参数传进来
    @Select("select sum(order_amount) order_amount from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date}")
    BigDecimal selectGMV(Integer date);

    // 获取某一天各个省份交易额
    // Mybatis会把每一条查询结果封装成一个对象, 最后全放在一个list集合里面

    // @Data
    // @AllArgsConstructor
    // TradeProvinceOrderAmount {
    //     // 省份名称
    //     String provinceName;
    //     // 下单金额
    //     Double orderAmount;
    // }

    @Select("select province_name,sum(order_amount) order_amount from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date} group by province_name")
    List<TradeProvinceOrderAmount> selectProvinceAmount(Integer date);
}