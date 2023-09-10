// @component
// 1. Controller    -> 表示层用这个注解
// 2. Service       -> 业务层用这个注解
// 3. Repository    -> 持久层用这个注解

// 将类对象的创建交给Spring的IOC容器, 对象创建好放在容器里面
// springboot在启动的时候会扫描注解

@RestController
FirstController {
    1. 例一
    // 接收客户端请求来进行处理
    // eg: http://localhost:8080/first?username=zs&password=123
    @RequestMapping("/first") -> 注解作用 -> 拦截客户端请求
    String first(String username, String password) {
        // @Controller的情况下如果返回值是字符串, 会进行页面跳转, 
        // 如果不想页面跳转只是做字符串的响应，可以在方法上加 @ResponseBody 或者用@RestController替换@Controller. @RestController = @Controller + @ResponseBody
        return "success";
    }

    2. 例二
    // @RequestParam(value = "hehe")会接收客户端请求的参数然后赋值给变量username
    // eg: http://localhost:8080/first?hehe=zs&password=123
    @RequestMapping("/first")
    String first(
        // 由于只有一个属性, 所以下面等价于@RequestParam("hehe"), 也就是可省略 'value = '
        @RequestParam(value = "hehe") String username, 
        // defaultValue = "baiyao" 给的默认值
        @RequestParam(value = "haha", defaultValue = "baiyao") String password
    ) {
        return "success";
    }
}