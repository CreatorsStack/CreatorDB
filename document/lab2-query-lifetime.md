# 一 一条 sql 在 simpleDB 中的生命周期

你是否在好奇, 一条 sql 语句到底是如何执行的?

你是否在疑问, 什么是 logical plan, 什么是 physical plan?

看完这篇文章, 一切都可以揭晓

## 前置工作

按照 lab2 2.7的指导, 需要先创建一个 data.txt 文件:

```
1,10
2,20
3,30
4,40
5,50
5,50
```

接着调用 simpleDB.main()  参数为

```
convert data.txt 2 "int,int"
```

这会将 data.txt 转化为 data.bat

接着创建 catalog.txt:

```
data (f1 int, f2 int)
```

调用 simpleDB.main()  参数为 :

```
parser catalog.txt
```

并输入一条 sql 语句:

```
"select d.f2, d1.f2 from data d, data d1 " +
"where d.f1 = d1.f1 and d.f2 > 20 " +
"order by d.f2 desc;";
```

这个 sql 语句的 query plan tree 为:

- 第一层是两个 scan 算子, 表都是 data
- 第二层是一个 filter 算子
- 第三层是一个 hash join 算子
- 第四层是 order by 算子
- 第五层是 projection 算子, 代表选择 d.f2, d1.f2 这两个 field

```
The query plan is:
             π(d.f2,d1.f2),card:1         --------------------    5
             |
             o(d.f2),card:1               --------------------   4
             |
          ⨝(hash)(d.f1=d1.f1),card:1     --------------------  3
   __________|___________
   |                    |
   σ(d.f2>20),card:1    |                 --------------------  2
   |                    | 
 scan(data d)         scan(data d1)       --------------------  1
```

接下来, 让我们来跟踪 simpleDB / parse.java 中的逻辑

## 算子简介

我们先来看看 lab2 中涉及到的一些算子

- SeqScan -- DataSource ：这个就是数据源，也就是表
- Filter：这个是 where 后面的过滤条件；
- Join: join 算子, 也是 where 后面解析出来的
- Aggregation：聚合算子, 主要包含两部分信息，一个是 Group by 后面的字段，以及 select 中的聚合函数的字段，以及函数等；
- Order : 排序算子
- Projection：这里就是对应的 select 后面跟的字段；

这些算子在 lab2 中都会涉及到, 其中有一些也需要我们实现

## 调用链

对于我们输入的一条 sql 语句, 其先后通过

SimpleDb.main()

->

Parser.main() -> Parser.start() -> Parser.processNextStatement() -> Parser.handleQueryStatement()

在 handleQueryStatement 中, 程序根据输入的 select 语句, 先后生成了逻辑执行计划和物理执行计划, 之后执行并输出结果

```
 // 生成 logical plan
 LogicalPlan lp = parseQueryLogicalPlan(tId, s);

// 转化 physicalPlan
OpIterator physicalPlan = lp.physicalPlan(tId, TableStats.getStatsMap(), explain);

...
query.execute();
```

## 生成 AST 语法树

对于我们输入的这条 sql 语句:

```
"select d.f2, d1.f2 from data d, data d1 " +
"where d.f1 = d1.f1 and d.f2 > 20 " +
"order by d.f2 desc;";
```

其会通过 zal.jar 这个 包, 生成一个 AST 语法树:

```
    Vector select_;
    boolean distinct_ = false;
    Vector from_;
    ZExp where_ = null;
    ZGroupBy groupby_ = null;
    ZExpression setclause_ = null;
    Vector orderby_ = null;
    boolean forupdate_ = false;
```

暂时可以认为, AST 语法树是一个结构体, 其解析了 sql 语句, 并转化成上面所示的字段



## 生成 logical plan

### what is logical plan?

简单来说, 就是把我们输入的 sql 语句翻译成一个结构体, 这个结构体叫做 LogicalPlan, 里面包含了输入的 sql 语句的每一个 '关键点'

logicalPlan 是从 Ast 转化来的

如下, 可以看到就是一些列的 node 和 field 组成的, 并没有什么特殊的地方

> 注: simpledb 只关注简单的 sql,  group by 和 aggregation 只能有一个

```
public class LogicalPlan {
    // Join node
    private List<LogicalJoinNode>             joins;
    
    // Table
    private final List<LogicalScanNode>       tables;
    
    // Filter node
    private final List<LogicalFilterNode>     filters;

	// Select node
    private final List<LogicalSelectListNode> selectList;
    
    // Group by field
    private String                            groupByField = null;
    
    // Aggregation field, such as sum()
    private boolean                           hasAgg       = false;
    private String                            aggOp;
    private String                            aggField;
    
    // Order by field
    private boolean                           oByAsc, hasOrderBy = false;
    private String                            oByField;
    
```

针对上文我们输入的那一条 sql, 会生成:

![image-20211210154359490](https://gitee.com/zisuu/mypicture/raw/master/image-20211210154359490.png)

![image-20211210154416709](https://gitee.com/zisuu/mypicture/raw/master/image-20211210154416709.png)

所以, 可以把生成 logical plan 的过程, 看作是一种 '翻译' 的过程.

### 流程概述

接下来, 让我们看看具体的生成步骤

- 生成 Scan node ---- 对应 from 后面的表名称
- 生成 Filter 和 join node --  对应 where 后面的 expression
- 生成 group by field -- 对应 group by
- 生成 agg field 和 selection node -- 对应 sum() 等 aggregation 和 select xxx
- 生成 order by field -- 对应 order by

在 simpleDb 中对应生成 logical plan 的函数是 physicalPlan.parseQueryLogicalPlan



## 生成 physical plan

### **what is physical plan ?**

在上面, 我们以及生成了一个 logical plan, 其就是一个普通的结构体, 包含一些 node 和 field

而 physical plan , 就是我们要生成的 operator tree

也即 从 logical plan -> physical plan , 就是生成一个 operator tree 的过程

对应上文的 sql 语句

```
"select d.f2, d1.f2 from data d, data d1 " +
"where d.f1 = d1.f1 and d.f2 > 20 " +
"order by d.f2 desc;";
```

会生成这么一颗树:

```
The query plan is:
             π(d.f2,d1.f2),card:1         --------------------    5
             |
             o(d.f2),card:1               --------------------   4
             |
          ⨝(hash)(d.f1=d1.f1),card:1     --------------------  3
   __________|___________
   |                    |
   σ(d.f2>20),card:1    |                 --------------------  2
   |                    | 
 scan(data d)         scan(data d1)       --------------------  1
```

这实际上就是一个根为 projection  的树结构

- projection 的 child 为 order 算子

- order 的 child 为 hash join 算子

- join 的 children 为 filter 和 scan 算子

这些算子我们在 lab2 里面都实现和接触过了, 相信你并不陌生

让我们在 debug 的信息中看一看:

![image-20211210160459834](C:\Users\黄章衡\AppData\Roaming\Typora\typora-user-images\image-20211210160459834.png)

### 流程概述

生成 logical plan 的流程比较简单, 让我们来看看生成 physical plan 的流程:

- 生成 d, d1 的seqScan 算子, 作为叶子算子, 放入 subplanMap , 其标识为 tableAlias. 这一步是为了确定输入源
- 生成 d 的filter 算子, 将 d.seqScan 作为 d.filter 的 child, 并代替 seqScan, 放入 subplanMap
- 生成 join 算子, 将 d. filter 算子 和  d1.seqScan 作为 join 的 children, 并删除 subplanMap 中多余 的算子, 只剩下一个 join 算子
- 遍历 selection 列表, 生成 projectoin 算子所需的 output fields 和 aggregation field
- 生成 aggregation 算子 (这个例子中没有), 将 join 作为其 child
- 生成 order by 算子, 将 join 作为其 child
- 最后生成 projection 算子

可以发现, 和生成 logical plan 的流程是可以对应起来的

对应的代码在 LogicalPlan.physicalPlan() 中



## 执行模型 -- volcano

ok, 目前为止, 我们以及生成了一个 projection 算子,其代表了一棵执行树:

```
The query plan is:
             π(d.f2,d1.f2),card:1         --------------------    5
             |
             o(d.f2),card:1               --------------------   4
             |
          ⨝(hash)(d.f1=d1.f1),card:1     --------------------  3
   __________|___________
   |                    |
   σ(d.f2>20),card:1    |                 --------------------  2
   |                    | 
 scan(data d)         scan(data d1)       --------------------  1
```

那么具体是如何执行的呢? 相信你在做 lab2 的各个算子的时候, 都可以看到, 每个算子都实现 OpIterator 接口, 实现了 next() 方法, 这实际上是课程中介绍的 '火山模型' -- volcano:

套用课程的 ppt, 举个例子:

每个 operator 都需要实现一个方法 next()

- next() 方法返回一个 tuple 或者 null
- 每个 operator 通过调用其 child operators 的 next() 方法, 来获取 tuples, 并处理他们

如下图: (采用 hash join)



[![img](https://gitee.com/zisuu/mypicture/raw/master/2282357-20210228200010429-1462288556.png)](https://img2020.cnblogs.com/blog/2282357/202102/2282357-20210228200010429-1462288556.png)

对于上面这个图的理解就是获取所有的`r.id`然后构建hash表

[![img](https://gitee.com/zisuu/mypicture/raw/master/2282357-20210228200017980-1884033487.png)](https://img2020.cnblogs.com/blog/2282357/202102/2282357-20210228200017980-1884033487.png)

然后在right的关系中获取出所有满足要求的`S.ID`

这里的`evalPred(t)`就等价于 `S.value > 100`

当然, 这种模型有一种缺陷就是, 性能太差, 于是课程中还介绍了 Materialization 和 Vectoriation 模型, 总体思想就是返回  batch tuple, 而不是 single tuple