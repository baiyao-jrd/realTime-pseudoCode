TrafficStatServiceImpl

    -> 流量域service接口实现

@Service
TrafficStatServiceImpl implements TrafficStatService {
    // trafficStatMapper属性的赋值, @Autowired -> 这个注解会将已经创建在容器中的tradeStatMapper对象找到并在这里赋值
    // 注入
    @Autowired
    TrafficStatMapper trafficStatMapper;

    List<TrafficUvCt> getChUv(Integer date, Integer limit) {
        return trafficStatMapper.selectChUv(date, limit);
    }
}