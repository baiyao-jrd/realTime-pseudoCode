总交易额需求分析

    -> 1. 组件: 数字翻牌器
       2. 接口地址与参数: http://localhost:8080/gmv?date=20220917
       3. 数据返回格式
            {
                "status": 0,
                "data": 1201004.4418
            }
        4. 执行的SQL: 
            select
                sum(order_amount) order_amount
            from dws_trade_province_order_window
            where toYYYYMMDD(stt) = 20220917