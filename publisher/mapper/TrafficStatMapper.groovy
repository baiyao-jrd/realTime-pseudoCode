TrafficStatMapper

    -> 获取某一天不同渠道的独立访客数
    // @Data
    // @AllArgsConstructor
    // TrafficUvCt {
    //     // 渠道
    //     String ch;
    //     // 独立访客数
    //     Integer uvCt;
    // }

interface TrafficStatMapper {
    @Select("SELECT  ch
                ,SUM(uv_ct) AS uv_ct
            FROM dws_traffic_vc_ch_ar_is_new_page_view_window
            WHERE toYYYYMMDD(stt) = #{date}
            GROUP BY ch
            HAVING uv_ct > 0
            ORDER BY uv_ct DESC
            LIMIT #{limit}")

    // mapper这里如果有多个参数, 应该给每个参数取个名字， 为了在select中使用#{}方式传参
    // 只有一个参数的情况下, 你sql里面无论传的是#{date}还是#{某某某}, 他都能生效, 多参的话就得指定了
    List<TrafficUvCt> selectChUv(@Param("date") Integer date, @Param("limit") Integer limit);
}