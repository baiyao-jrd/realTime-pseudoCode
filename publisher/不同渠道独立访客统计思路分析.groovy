不同渠道独立访客统计思路分析

    -> 1. 组件: 玫瑰饼图
       2. 接口地址与参数: http://localhost:8080/ch?date=20220917&limit=5
       3. 数据返回格式
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
        4. 执行的SQL: 
            SELECT  ch
                ,SUM(uv_ct) AS uv_ct
            FROM dws_traffic_vc_ch_ar_is_new_page_view_window
            WHERE toYYYYMMDD(stt) = 20220901
            GROUP BY ch
            HAVING uv_ct > 0
            ORDER BY uv_ct DESC
            LIMIT 5

            // ch           uv_ct
            // Appstore     97
            // xiaomi       70
            // oppo         51
            // wandoujia    26
            // web          14