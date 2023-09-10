TrafficStatController

    -> 控制层

@RestController
TrafficStatController {

    @Autowired
    private TrafficStatService trafficStatService;

    // 拦截请求
    @RequestMapping("/ch")
    String getChUv(
        // 前台请求没有传这个参数, 给定默认值
        @RequestParam(value = "date", defaultValue = "0") Integer date,
        @RequestParam(value = "limit", defaultValue = "20") Integer limit
    ) {
        if (date == 0) {
            date = Integer
                .valueOf(
                    DateFormatUtils.format(new Date(), "yyyyMMdd")
                );
        }

        List<TrafficUvCt> trafficUvCtList = trafficStatService.getChUv(date, limit);
        
            {
                "status": 0,
                "data": [
                    {
                        "name": "搜索引擎",
                        "value": 360
                    },
                    {
                        "name": "直接访问",
                        "value": 335
                    }
                ]
            }

        StringBuilder json = new StringBuilder("{\"status\": 0,\"data\": [");

        for (int i = 0; i < trafficUvCtList.size(); i++) {
            TrafficUvCt trafficUvCt = trafficUvCtList.get(i);
            json.append("{\"name\": \""+ trafficUvCt.getCh()+"\",\"value\": "+ trafficUvCt.getUvCt() + "}");

            // 不同渠道, 逗号分割
            if(i < trafficUvCt.size() - 1){
                json.append(",");
            }
        }
        
        json.append("] }");

        return json.toString();
    }
}
