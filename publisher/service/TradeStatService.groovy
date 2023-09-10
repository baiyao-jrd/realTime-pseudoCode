TradeStatService
    -> Service层

interface TradeStatService {
    //获取某一天总交易额
    BigDecimal getGMV(Integer date);

    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}
