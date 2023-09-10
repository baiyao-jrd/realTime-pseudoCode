TradeStatController

    -> 控制层

@RestController
TradeStatController {

    @Autowired
    private TradeStatService tradeStatService;

    // 拦截请求
    @RequestMapping("/gmv")
    // 日期为传, 给个默认值
    // 这里"0"会被springMVC转换为Integer的0
    String getGmv(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        //判断日期是否为null
        if (date == 0) {
            date = Integer
                .valueOf(
                    DateFormatUtils.format(new Date(), "yyyyMMdd")
                );
        }

        BigDecimal gmv = tradeStatService.getGMV(date);
        
        String json = "{\"status\": 0,\"data\": " + gmv + "}";
        
        return json;
    }

    @RequestMapping("/province")
    public Map getProvinceAmount(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = Integer
                .valueOf(
                    DateFormatUtils.format(new Date(), "yyyyMMdd")
                );
        }
        
        List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStatService.getProvinceAmount(date);

        Map resMap = new HashMap();
        resMap.put("status",0);
        Map dataMap = new HashMap();
        List mapDataList = new ArrayList();
        for (TradeProvinceOrderAmount orderAmount : provinceOrderAmountList) {
            Map provinceMap = new HashMap();
            provinceMap.put("name",orderAmount.getProvinceName());
            provinceMap.put("value",orderAmount.getOrderAmount());
            mapDataList.add(provinceMap);
        }
        dataMap.put("mapData",mapDataList);
        dataMap.put("valueName","交易额");
        resMap.put("data",dataMap);
        return resMap;
    }

}
