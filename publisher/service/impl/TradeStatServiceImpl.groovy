TradeStatServiceImpl
    -> 交易域统计Service接口实现类

// @Service: 表示当前是一个service层的处理程序 -> 类对象的创建是要交给容器去管理的
@Service
TradeStatServiceImpl implements TradeStatService {
    // tradeStatMapper属性的赋值, @Autowired -> 这个注解会将已经创建在容器中的tradeStatMapper对象找到并在这里赋值
    // 注入
    @Autowired
    TradeStatMapper tradeStatMapper;

    BigDecimal getGMV(Integer date) {
        return tradeStatMapper.selectGMV(date);
    }

    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date) {
        return tradeStatMapper.selectProvinceAmount(date);
    }
}