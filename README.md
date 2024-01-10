
## 项目结构介绍

### doit30
参考 https://github.com/coderblack/doit30_flink

### flink-sql
读取配置sql文件，使用Table API执行Job      

### flink-learn     
com.yzhou.scala.sqljoin : 双流 join 的学习案例     

### Flink 异步 I/O    

案例1：消费 Kafka，根据数据 id 关联 商品名称信息  

### flink-jar   
用于偏生产级别 jar应用的开发模板      

1.解决 main(args) 可根据不同环境引入不同参数，此处仍需注意参数优先级   


### 案例分析    
* 案例：消费 Kafka，根据数据 id 关联 商品名称信息（KafkaWideColumnMySQL2App.java）
* 案例：规则：用户如果在 10s 内，同时输入 TMD 超过 5 次，就认为用户为恶意攻击，识别出该用户（BarrageBehavior01.java）
* 案例：规则：用户如果在 10s 内，同时连续输入同样一句话超过 5 次，就认为是恶意刷屏。（BarrageBehavior02.java）
* 案例：活动页面统计，A1 表示活动，View 表示用户行为，统计 活动页面的 PV，UV（ActivityCount.java）


