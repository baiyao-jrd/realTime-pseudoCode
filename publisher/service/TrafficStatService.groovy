TrafficStatService

    -> 流量域统计Service接口

interface TrafficStatService {
    List<TrafficUvCt> getChUv(Integer date, Integer limit);
}