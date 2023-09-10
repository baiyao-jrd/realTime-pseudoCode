Gmall0321PublisherApplication
    -> springboot入口类

@SpringBootApplication
// 可以让当前这个程序在启动的时候扫描TradeStatMapper接口所在的包com.atguigu.gmall.publisher.mapper
// spring的IOC容器就会自动的把这个接口的实现类给创建出来, 并放在容器中进行管理
// 它会将接口中的抽象方法进行实现, jdbc的那一套它会实现出来 -> 有注解@Select("")的方法
@MapperScan(basePackages = "com.atguigu.gmall.publisher.mapper")
Gmall0321PublisherApplication {
    main(String[] args) { SpringApplication.run(Gmall0321PublisherApplication.class, args); }
}
