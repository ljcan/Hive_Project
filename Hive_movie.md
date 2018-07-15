
### Hive使用Python对电影浏览量以及评分进行数据分析统计

**需求分析**

1. 根据用户观看的日期的时间分析在一周中哪一天观看电影的用户最多，也就是电影浏览量峰值。
2. 统计分析电影评分排行榜TOP10（后续可以对用户进行电影推荐）

**数据的采集**

可以在 http://files.grouplens.org/datasets/movielens/ 网站上下载ml-100k.zip数据集压缩包

下载好数据集之后，解压里面有一个u.data的数据集，里面的数据格式如下所示：
```
userid  movieid  rate   time
196     242     3       881250949
186     302     3       891717742
22      377     1       878887116
244     51      2       880606923
```

上面的数据一次是用户的ID，观看电影的ID，用户对于电影的评分，观看电影的日期，u.data文件中的有10万条数据，下面我们会这个数据集进行分析。

**创建python脚本**

数据清洗： 创建python脚本对数据的日期进行转换

```
import sys
import datetime

for line in sys.stdin:
  line = line.strip()
  userid, movieid, rating, unixtime = line.split('\t')
  weekday = datetime.datetime.fromtimestamp(float(unixtime)).isoweekday()
  print '\t'.join([userid, movieid, rating, str(weekday)])
```

将上面的代码写入一个python文件中保存。

**创建表以及加载数据**

```
CREATE TABLE u_data (
  userid INT,
  movieid INT,
  rating INT,
  unixtime STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/opt/datas/ml-100k/u.data' OVERWRITE INTO TABLE u_data;
```

**数据清洗**

```
CREATE TABLE u_data_new (
userid INT,
movieid INT,
rating INT,
weekday INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

add FILE /opt/datas/weekday_mapper.py;

INSERT OVERWRITE TABLE u_data_new
SELECT
TRANSFORM (userid, movieid, rating, unixtime)
USING 'python weekday_mapper.py'
AS (userid, movieid, rating, weekday)
FROM u_data;
```

**数据分析**

1.需求一：分析电影浏览量峰值：

```
SELECT weekday, COUNT(1) cnt FROM u_data_new GROUP BY weekday order by cnt desc;
```

分析结果如下所示：

```
weekday cnt
5       17964
3       15426
2       14816
4       13774
1       13278
7       12424
6       12318
```

由上结果可以看出来，在星期五用户对电影的点击量最高。那么我们可以对这个结果一些处理，比如在这一时间段向用户推荐更多的电影来获取更多的利益。

2.需求二：分析电影最高评分榜TOP10

```
SELECT movieid,rating  FROM u_data_new order by rating desc limit 10;
```

分析结果如下：

```
movieid rating
69      5
100     5
12      5
405     5
238     5
302     5
24      5
866     5
867     5
228     5
```

从结果可以看出电影评分榜TOP10的电影评分都是5分，对应的我们也可以知道它的电影ID，那么我们也可以通过这一结果对用户进行电影推荐。



 > JUST-2016-不清不慎   
###  联系方式：邮箱:2671268148@qq.com  
###  微信公众号：【不清不慎的博客】
###  请访问：【[不清不慎CSDN博客地址](https://blog.csdn.net/qq_37142346)】













