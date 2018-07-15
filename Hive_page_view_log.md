
## Hive进行离线日志文件分析

**需求分析**

1. 分析客户浏览该网站集中分布的时间段。
2. 分析点击量最高的IP地址，即地域分析（可以向该地区用户多推广）。

**数据采集**

现在大多数网站都采用nginx服务器产生日志文件，因此我们可以使用nginx服务器日志文件进行日志分析，大概有10万条数据。

日志文件格式如下：

```
"27.38.5.159" "-" "31/Aug/2015:00:04:37 +0800" "GET /course/view.php?id=27 HTTP/1.1" "303" "440" - "http://www.xxxxx.com/user.php?act=mycourse" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36" "-" "xxxxx.com"
"27.38.5.159" "-" "31/Aug/2015:00:04:37 +0800" "GET /login/index.php HTTP/1.1" "303" "465" - "http://www.xxxxx.com/user.php?act=mycourse" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36" "-" "xxxx.com"
"27.38.5.159" "-" "31/Aug/2015:00:04:53 +0800" "GET /course/view.php?id=27 HTTP/1.1" "200" "7877" - "http://www.xxxx.com/user.php?act=mycourse&testsession=1637" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36" "-" "xxxxx.com"

```

上面是部分日志文件的格式，上面的日志文件中格式中字段很多，但是我们需要的只有几个，比如第一个字段"27.38.5.159"就是用户ip地址，"31/Aug/2015:00:04:37 +0800"为用户访问时间，"GET /course/view.php?id=27 HTTP/1.1"为用户的请求方式以及发送请求的地址，"http://www.xxxxx.com/user.php?act=mycourse" 为用户访问的URL，后面的字段都是一些浏览器相关的信息，这些信息我们并不需要，因此在后续会进行数据清洗保留有用的字段。

**创建表以及加载数据**

根据上面日志文件格式编写正则表达式并且创建表：

```
create table page_log(
user_ip string,
username string,
user_time string,
request_url string,
request_state string,
request_port string,
limited string,
des_url string,
brower string,
brower_limit string,
to_url string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "(\"[^ ]*\") (\"-|[^ ]*\") (\"[^\]]*\") (\"[^\"]*\") (\"[0-9]*\") (\"[0-9]*\") (-|[^ ]*) (\"[^ ]*\") (\"[^\"]*\") (-|[^ ]*) (\"[^ ]*\")"
)
STORED AS TEXTFILE;

load data local inpath '/opt/datas/page_view.log' into table page_log;
```

抽取有用的字段利用SNAPPY压缩的方式创建一张新表：

```
drop table if exists page_log_comm;
create table page_log_comm(
user_ip string,
user_time string,
request_url string,
des_url string
)
row format delimited fields terminated by '\t'
stored as orc tblproperties("orc.compress"="SNAPPY");

insert into table page_log_comm select user_ip,user_time,request_url,des_url from page_log;
```

**数据清洗**

下面我们使用UDF编程来进行数据清洗：
1.对上面日志文件中的日期进行格式的转换，如"31/Aug/2015:00:04:37 +0800"的格式转换为20150831000437
字符串：

```
package cn.just.hive.udf;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
/**
 * 1. Implement one or more methods named
 * "evaluate" which will be called by Hive.
 * 2."evaluate" should never be a void method. However it can return "null" if
 * needed.
 * @author shinelon
 *
 */

//@Description() 利用该注解编写函数注释
public class DateFormat extends UDF{
	private final SimpleDateFormat inputFormat=new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
	private final SimpleDateFormat outputFormat=new SimpleDateFormat("yyyyMMddHHmmss");
	 public Text evaluate(Text input) {
		 Text output=new Text();
		 if(null==input) {
			 return null;
		 }
		 String inputDate=input.toString().trim();
		 if(null==input.toString()) {
			 return null;
		 }
		 try {
			 Date parseDate=inputFormat.parse(inputDate);
			 String outputDate=outputFormat.format(parseDate);
			 output.set(outputDate);
		 }catch (Exception e) {
			 e.printStackTrace();
			 return output;
		 }
		 return output;
	 }
	 public static void main(String[] args) {
		System.out.println(new DateFormat().evaluate(new Text("31/Aug/2015:00:04:37 +0800")));
	}
}
```

2.除去上面日志文件中每一列数据中的引号：

```
package cn.just.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
/**
 * 1. Implement one or more methods named
 * "evaluate" which will be called by Hive.
 * 2."evaluate" should never be a void method. However it can return "null" if
 * needed.
 * @author shinelon
 *
 */
public class RemoveQuotesUDF extends UDF{
	 public Text evaluate(Text str) {
		 if(null==str) {
			 return new Text();
		 }
		 if(null==str.toString()) {
			 return new Text();
		 }
		 return new Text(str.toString().replaceAll("\"",""));
	 }
	 public static void main(String[] args) {
		System.out.println(new RemoveQuotesUDF().evaluate(new Text("\"31/Aug/2015:23:57:46 +0800\"")));
	}
}
```
编写好着两个函数之后，将其打成jar包，下面Hive使用上面编写好的函数进行数据分析：

```
###################数据清洗之自定义UDF函数来去除引号####################
add jar /opt/datas/hive_udf2.jar;
list jars;
create temporary funtion removequote as "cn.just.hive.udf.RemoveQuotesUDF";
insert overwrite table page_log_comm select removequote(user_ip),removequote(user_time),removequote(request_url),removequote(des_url) from page_log;

#################定义日期格式转换函数################################
add jar /opt/datas/hive_udf3.jar;
create temporary function my_dateformat as 'cn.just.hive.udf.DateFormat';
insert overwrite table page_log_comm select removequote(user_ip),my_dateformat(removequote(user_time)),removequote(request_url),removequote(des_url) from page_log;

```

**数据分析**

1. 分析客户浏览该网站集中分布的时间段。

```
select t.hour,count(*) cnt from
(select substring(user_time,9,2) hour from page_log_comm) t
group by t.hour order by cnt desc;
```

分析结果如下所示：

```
t.hour  cnt
15      25619
17      7888
16      7174
10      6012
20      5763
09      5544
22      5508
14      5232
19      5219
18      5202
21      5134
11      4590
13      2430
12      2232
08      2214
23      1292
00      1260
06      882
07      198
01      198
        157
02      126
04      72
05      36
03      18
```

由上面结果可以看出用户在15时访问访问网站的浏览量最大，点击量为25619。

2. 分析点击量最高的IP地址:

```
select t.pre_ip,count(*) cnt from
(select substring(user_ip,1,7) pre_ip from page_log_comm) t
group by t.pre_ip order by cnt desc;
```

分析结果如下所示：

```
t.pre_ip        cnt
180.173 38338
218.88. 5236
183.143 5168
180.158 3077
183.62. 3041
61.157. 2857
183.37. 2567
113.98. 2550
220.161 2221
112.64. 1998
110.90. 1853
61.154. 1815
101.199 1806
180.153 1391
116.216 1358
183.61. 1355
116.227 1343
124.166 1206
218.85. 1190
117.114 1163
60.30.2 1044
101.226 810
171.91. 764
222.168 648
183.12. 612
114.111 612
```

从分析结果来看，访问量最多的用户集中在ip为180.173.0.0的地域，而经过查询该ip地址对应于上海，因此我们可以针对上海的客户进行商业销售从而获取商业利益。


 > JUST-2016-不清不慎   
###  联系方式：邮箱:2671268148@qq.com  
###  微信公众号：【不清不慎的博客】
###  请访问：【[不清不慎CSDN博客地址](https://blog.csdn.net/qq_37142346)】




