# 一、实验概览

这个实验需要完成的内容有：

1. 实现过滤、连接运算符，这些类都是继承与OpIterator接口了，该实验提供了OrderBy的操作符实现，可以参考实现。最终的SQL语句解析出来都是要依靠这些运算符的；
2. 实现聚合函数，由于该数据库只有int和string两种类型，int类型可实现的聚合函数有max,min,avg,count等，string类型只需要实现count；这些与分组查询一起使用，选择进行聚合操作时，可以选择是否进行分组查询。
3. 对IntegerAggregator和StringAggregator的封装，查询计划是调用Aggregate，再去调用具体的聚合器，最后获得聚合结果。
4. 实现插入、删除记录。包括从HeapPage、HeapFile、BufferPool中删除，这里需要把三个之间的调用逻辑搞清楚，代码会好写很多。
5. 实现BufferPool的数据页淘汰策略。我在 Lab1 中已经用 Lru 实现了.

# 二、Guideline

## Exercise1:Filter and Join

exercise1要求我们完成Filter和Join两种操作符，下面是相关描述：

- *Filter*: This operator only returns tuples that satisfy a `Predicate` that is specified as part of its constructor.
  Hence, it filters out any tuples that do not match the predicate.
- *Join*: This operator joins tuples from its two children according to a `JoinPredicate` that is passed in as part of
  its constructor. We only require a simple nested loops join, but you may explore more interesting join
  implementations. Describe your implementation in your lab writeup.

### Filter

Filter是SQL语句中where的基础，如select * from students where id > 2, Filter 起到条件过滤的作用, 也即过滤出来所有满足 id > 2 的 tuple。

```
    private Predicate         predicate;
    private OpIterator        child;
    private TupleDesc         tupleDesc;
```

其中，predicate是断言，实现条件过滤的重要属性；而child是数据源，我们从这里获取一条一条的Tuple用predicate去过滤；td是我们返回结果元组的描述信息，在Filter中与传入的数据源是相同的，而在其它运算符中是根据返回结果的情况去创建TupleDesc的；

FetchNext 方法如下:

```
protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException, DbException {
    // some code goes here
    Tuple tuple;
    while (this.child.hasNext()) {
        tuple = this.child.next();
        if (tuple != null) {
            if (this.predicate.filter(tuple)) {
                return tuple;
            }
        }
    }
    return null;
}
```



### Predicate

我们通过it.next()去获取一条一条符合过滤条件的Tuple。而其中的过滤的实现细节就是通过Predicate来实现的：

            if (this.predicate.filter(tuple)) {
                return tuple;
            }

可以看到，每次调用fetchNext，我们是从Filter的child数据源中不断取出tuple，只要有一条Tuple满足predicate的filter的过滤条件，我们就可以返回一条Tuple，即这条Tuple是经过过滤条件筛选之后的有效Tuple。

Predicate的基本属性如下：

```
private int   field;
private Op    op;
private Field operand;
```

其中 field 代表要比较 tuple 的第几个字段，op表示具体的运算符, 包括：相等、大于、小于、等于、不等于、大于等于、小于等于、模糊查询这几种。

而operand是用于参与比较的，比如上述SQL语句select * from students where id > 2；假如id是第0个字段，这里的field = 0，op = GREATER_THAN（大于），operand = new IntField(1)。这里进行比较过滤的实现在filter方法中:

```
public boolean filter(Tuple t) {
    // some code goes here
    final Field field1 = t.getField(this.field);
    final Field field2 = this.operand;
    return field1.compare(this.op, field2);
}
```

可以看到，Predicate的作用就是将传入的Tuple进行判断，而Predicate的field属性表明使用元组的第几个字段去与操作数operand进行op运算操作，比较的结果实际是调用Field类的compare方法，compare方法会根据传入的运算符和操作数进行比较，以IntField为例：

```
    public boolean compare(Predicate.Op op, Field val) {

        IntField iVal = (IntField) val;

        switch (op) {
            case EQUALS:
            case LIKE:
                return value == iVal.value;
            case NOT_EQUALS:
                return value != iVal.value;
            case GREATER_THAN:
                return value > iVal.value;
            case GREATER_THAN_OR_EQ:
                return value >= iVal.value;
            case LESS_THAN:
                return value < iVal.value;
            case LESS_THAN_OR_EQ:
                return value <= iVal.value;
        }

        return false;
    }

```

可以看到支持的运算符有相等、大于、小于、不等于、大于等于、小于等于这些运算符，这里LIKE和EQUALS都表示等于的意思。

### OrderBy

实验提供了OrderBy的实现，其思路与我们实现的Filter也是相似的，区别在于对fetchNext的获取下一条tuple的实现有所不同。OrderBy的属性如下：

```
    private static final long serialVersionUID = 1L;
    private OpIterator        child;
    private final TupleDesc   td;
    private final List<Tuple> childTups        = new ArrayList<>();
    private final int         orderByField;
    private final String      orderByFieldName;
    private Iterator<Tuple>   it;
    private final boolean     asc;
```

关键的属性：

1、child：数据源，传入进行排序的所有记录Tuple；

2、childTups：OrderBy的实现思路是在open时将数据源child的所有记录存入list中，然后进行排序；

3、asc：升序还是降序，true表示升序；

4、orderByField：依据元组的第几个字段进行排序；

5、it：对childTups进行排序后childTups.iterator()返回的迭代器，原数据源child依据field字段进行排序后的所有数据。

这里主要看open的实现，因为在open中实现了排序并存入it迭代器中，后续调用fetchNext只需要在it中取就行了：

```
public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
    child.open();
    // load all the tuples in a collection, and sort it
    while (child.hasNext())
        childTups.add(child.next());
    childTups.sort(new TupleComparator(orderByField, asc));
    it = childTups.iterator();
    super.open();
}
```

fetchNext就简单很多了，直接从结果迭代器中取就完事了：

```
protected Tuple fetchNext() throws NoSuchElementException {
    if (it != null && it.hasNext()) {
        return it.next();
    } else
        return null;
}
```

实际上, 在整个实现的过程中, 像 join, order by , aggregation 这些算子, 都是需要在 open 的时候就直接计算好结果.

### Join

理解了上面Filter与Predicate的关系以及OrderBy的实现思路，来做Join和JoinPredicate就会容易一点点了。

Join是连接查询实现的基本操作符，我们在MySQL中会区分内连接和外连接，我们这里只实现内连接。一条连接查询的SQL语句如下：

select a.*,b.* from a inner join b on a.id=b.id

Join的主要属性如下：

```
    private JoinPredicate     joinPredicate;
    private OpIterator        child1;
    private OpIterator        child2;
    private TupleDesc         td;

    private JoinStrategy      joinStrategy;
    private TupleIterator     iterator;
```

其中child1，child2是参与连接查询的两个表的元数据，从里面取出tuple使用joinPredicate进行连接过滤。td是结果元组的描述信息，使用内连接我们是将两个表连起来，所以如果显示连接两个表的所有字段的记录，td可以简单理解成两个child数据源的两种tuple的td的拼接

JoinStrategy 是我自己添加的一个接口, 由不同的实现类, 包括 sortMergeJoin , nestedLoopJoin, hashJoin 等等



可以在 open 的时候指定 join strategy:

```
public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
    // some code goes here
    this.child1.open();
    this.child2.open();
    super.open();
    
    // You can choose sortMerge join, hash join, or nested loop join
    this.joinStrategy = new NestedLoopJoin(child1, child2, this.td, this.joinPredicate);
    this.iterator = this.joinStrategy.doJoin();
    this.iterator.open();
}
```

#### nestedLoopJoin

我们来看一下简单的 nestedLoopJoin 算法:

```
    @Override
    public TupleIterator doJoin() {
        final List<Tuple> tuples = new ArrayList<>();
        try {
            child1.rewind();
            while (child1.hasNext()) {
                final Tuple lTuple = child1.next();
                child2.rewind();
                while (child2.hasNext()) {
                    final Tuple rTuple = child2.next();
                    if (this.joinPredicate.filter(lTuple, rTuple)) {
                        tuples.add(mergeTuple(lTuple, rTuple, this.td));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error happen when nested loop join");
        }
        return new TupleIterator(this.td, tuples);
    }

```

#### SortMergeJoin

主要分为几个步骤:

- 构建两个 block 缓冲块
- 对于 输入源 child1, 利用 block1 缓冲其每一个 block, 然会遍历 child2 的每一个 block, 进行 sortMergeJoin:
    - 先对两个 block 进行排序
    - 然后利用双指针算法, 进行匹配输出即可

```
    private Tuple[]       block1;
    private Tuple[]       block2;

    private JoinPredicate lt;
    private JoinPredicate eq;
    
    
    @Override
    public TupleIterator doJoin() {
        final List<Tuple> tupleList = new ArrayList<>();

        // fetch child1
        try {
            child1.rewind();
            while (child1.hasNext()) {
                int end1 = fetchTuples(child1, block1);
                // Fetch each block of child2, and do merge join
                child2.rewind();
                
                while (child2.hasNext()) {
                    int end2 = fetchTuples(child2, block2);
                    
                    // sortMerge
                    mergeJoin(tupleList, end1, end2);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error happen when sort merge join:" + e.getMessage());
        }
        Arrays.fill(this.block1, null);
        Arrays.fill(this.block2, null);
        return new TupleIterator(this.td, tupleList);
    }

    private void mergeJoin(final List<Tuple> tupleList, int end1, int end2) {
        // 1.Sort each block
        final int field1 = this.joinPredicate.getField1();
        final int field2 = this.joinPredicate.getField2();
        sortTuples(block1, end1, field1);
        sortTuples(block2, end2, field2);

        // 2.Join
        int index1 = 0, index2 = 0;
        final Predicate.Op op = this.joinPredicate.getOperator();
        switch (op) {
            case EQUALS: {
                while (index1 < end1 && index2 < end2) {
                    final Tuple lTuple = this.block1[index1];
                    final Tuple rTuple = this.block2[index2];
                    if (eq.filter(lTuple, rTuple)) {
                        // If equal , we should find the right boundary that equal to lTuple in block1 and rTuple in block2
                        final JoinPredicate eq1 = new JoinPredicate(field1, Predicate.Op.EQUALS, field1);
                        final JoinPredicate eq2 = new JoinPredicate(field2, Predicate.Op.EQUALS, field2);
                        int begin1 = index1 + 1, begin2 = index2 + 1;
                        while (begin1 < end1 && eq1.filter(lTuple, this.block1[begin1]))
                            begin1++;
                        while (begin2 < end2 && eq2.filter(rTuple, this.block2[begin2]))
                            begin2++;
                        for (int i = index1; i < begin1; i++) {
                            for (int j = index2; j < begin2; j++) {
                                tupleList.add(mergeTuple(this.block1[i], this.block2[j], this.td));
                            }
                        }
                        index1 = begin1;
                        index2 = begin2;
                    } else if (lt.filter(lTuple, rTuple)) {
                        index1++;
                    } else {
                        index2++;
                    }
                }
                return;
            }
            case LESS_THAN:
            case LESS_THAN_OR_EQ: {
                while (index1 < end1) {
                    final Tuple lTuple = this.block1[index1++];
                    while (index2 < end2 && !this.joinPredicate.filter(lTuple, this.block2[index2]))
                        index2++;
                    while (index2 < end2) {
                        final Tuple rTuple = this.block2[index2++];
                        tupleList.add(mergeTuple(lTuple, rTuple, this.td));
                    }
                }
                return;
            }
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ: {
                while (index1 < end1) {
                    final Tuple lTuple = this.block1[index1++];
                    while (index2 < end2 && this.joinPredicate.filter(lTuple, this.block2[index2]))
                        index2++;
                    for (int i = 0; i < index2; i++) {
                        final Tuple rTuple = this.block2[i];
                        tupleList.add(mergeTuple(lTuple, rTuple, this.td));
                    }
                }
            }
        }
    }

    private void sortTuples(final Tuple[] tuples, int field, int len) {
        final JoinPredicate lt = new JoinPredicate(field, Predicate.Op.LESS_THAN, field);
        final JoinPredicate gt = new JoinPredicate(field, Predicate.Op.GREATER_THAN, field);
        Arrays.sort(tuples, 0, len, (o1, o2) -> {
            if (lt.filter(o1, o2)) {
                return -1;
            }
            if (gt.filter(o1, o2)) {
                return 1;
            }
            return 0;
        });
    }
```



#### JoinPredicate

joinPredicate 和 predicate 差不多, 只不过要对两个 tuple 进行比较:

```
public boolean filter(Tuple t1, Tuple t2) {
    // some code goes here
    return t1.getField(this.field1).compare(this.op, t2.getField(this.getField2()));
}
```

## Exercise2:Aggregates

exercise2的介绍：

> An additional SimpleDB operator implements basic SQL aggregates with a
> `GROUP BY` clause. You should implement the five SQL aggregates
> (`COUNT`, `SUM`, `AVG`, `MIN`,
> `MAX`) and support grouping. You only need to support aggregates over a single field, and grouping by a single field.
>
> In order to calculate aggregates, we use an `Aggregator(聚合器)`
> interface which merges a new tuple into the existing calculation of an aggregate. The `Aggregator` is told during
> construction what operation it should use for aggregation. Subsequently, the client code should
> call `Aggregator.mergeTupleIntoGroup()` for every tuple in the child iterator. After all tuples have been merged, the
> client can retrieve a OpIterator of aggregation results. Each tuple in the result is a pair of the
> form `(groupValue, aggregateValue)`, unless the value of the group by field was `Aggregator.NO_GROUPING`, in which case
> the result is a single tuple of the form `(aggregateValue)`.
>
> Note that this implementation requires space linear in the number of distinct groups. For the purposes of this lab, you
> do not need to worry about the situation where the number of groups exceeds available memory.

exerciese2要求我们实现各种聚合运算如count、sum、avg、min、max等，并且聚合器需要拥有分组聚合的功能。如以下SQL语句：

```mysql
SELECT SUM(fee) AS country_group_total_fee, country FROM member GROUP BY country
```

这条语句的功能是查询每个国家的费用总和及国家名称(根据国家名称进行分组)，这里用到了聚合函数SUM。其中fee是聚合字段，country是分组字段，这两个字段是我们理解聚合运算的关键点。

You only need to support aggregates over a single field, and grouping by a single field.讲义告诉我们，我们只需实现根据一个字段去分组和聚合，也就是只有一个分组字段和一个聚合字段。

exercise2的实验要求：

Implement the skeleton methods in:

------

- src/java/simpledb/execution/IntegerAggregator.java
- src/java/simpledb/execution/StringAggregator.java
- src/java/simpledb/execution/Aggregate.java

------

At this point, your code should pass the unit tests IntegerAggregatorTest, StringAggregatorTest, and AggregateTest.
Furthermore, you should be able to pass the AggregateTest system test.

### IntegerAggregator

IntegerAggregator的本质是一个聚合器, 其对传入的 Tuple , 根据 group by 字段进行 merge 操作, 将 tuple 的值合并到之前的信息中

为此, 我构建了一个 aggInfo 类, 用于保存 AggInfo 的信息:

```
private static class AggInfo {
    int cnt;
    int sum;
    int max = Integer.MIN_VALUE;
    int min = Integer.MAX_VALUE;
}
```

IntegerAggregator 的基本属性如下:

```
private Map<Field, AggInfo> groupMap;
// Group by field
private int                 gbField;
private Type                gbFieldType;

// Aggregation field
private int                 agField;

// Aggregation operation
private Op                  op;

private TupleDesc           td;
```



- 其中，gbField 是指依据 tuple 的第几个字段进行聚合操作，当无需分组时groupField的值为-1，在上面的SQL语句中相当于country这个字段；gbFieldType 是分组字段的类型，如果无需分组这个属性值为null；

- agField是指对tuple的第几个字段进行聚合，在上面的SQL语句中相当于fee字段；
- op是进行聚合运算的操作符，相当于上述SQL语句的SUM。
- td是结果元组的描述信息，对于有分组的聚合运算，td是一个拥有两个字段的TupleDesc，以(groupField, aggResult)的形式，保存原tuple进行分组聚合后每个分组对应的聚合结果，对于没有分组的聚合运算，td只有一个字段来保存聚合结果；
- groupMap 用于保存聚合的结果集，后面进行运算会用到。

不管是IntegerAggregator还是StringAggregator，他们的作用都是进行聚合运算（分组可选），所以他们的核心方法在于mergeTupleIntoGroup。IntegerAggregator.mergeTupleIntoGroup(Tuple tup)的实现思路是这样的：

1.根据构造器给定的 agField 获取在tup中的聚合字段及其值；

2.根据构造器给定的groupField获取tup中的分组字段，如果无需分组，则为null；这里需要检查获取的分组类型是否正确；

3.根据构造器给定的aggregateOp进行分组聚合运，我们将结果保存在groupMap中，key是分组字段(如果无需分组则为null)，val是聚合结果；

下面是具体代码：

```java
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.td == null) {
            buildTupleDesc(tup.getTupleDesc());
        }
        final IntField agField = (IntField) tup.getField(this.agField);
        final Field gbField = tup.getField(this.gbField);
        if (this.gbField != NO_GROUPING) {
            doAggregation(gbField, agField.getValue());
        } else {
            doAggregation(DEFAULT_FIELD, agField.getValue());
        }
    }

    private void doAggregation(final Field key, final int value) {
        if (key != null) {
            AggInfo preInfo = this.groupMap.getOrDefault(key, new AggInfo());
            switch (this.op) {
                case MIN: {
                    preInfo.min = Math.min(preInfo.min, value);
                    break;
                }
                case MAX: {
                    preInfo.max = Math.max(preInfo.max, value);
                    break;
                }
                case AVG: {
                    preInfo.sum += value;
                    preInfo.cnt += 1;
                    break;
                }
                case SUM: {
                    preInfo.sum += value;
                    break;
                }
                case COUNT: {
                    preInfo.cnt += 1;
                    break;
                }
            }
            this.groupMap.put(key, preInfo);
        }
    }

```

IntegerAggregator的另一个关键的方法是iterator方法，它用于把聚合的结果封装成tuple然后以迭代器的形式返回

下面是具体实现代码：

返回的td是一个拥有两个字段的TupleDesc，以(groupField, aggResult)的形式，保存原tuple进行分组聚合后每个分组对应的聚合结果，对于没有分组的聚合运算，td只有一个字段来保存聚合结果；

```java
    public OpIterator iterator() {
        // some code goes here
        final List<Tuple> tuples = new ArrayList<>();
        if (this.gbField != NO_GROUPING) {
            this.groupMap.forEach((key, info) -> {
                final Tuple tuple = new Tuple(this.td);
                tuple.setField(0, key);
                tuple.setField(1, new IntField(parseValue(key)));
                tuples.add(tuple);
            });
        } else {
            final Tuple tuple = new Tuple(this.td);
            tuple.setField(0, new IntField(parseValue(DEFAULT_FIELD)));
            tuples.add(tuple);
        }
        return new TupleIterator(this.td, tuples);
    }
```

### Aggregate

上面说到，AVG运算当需要获取聚合结果时，再进行计算返回，那么在哪里会来获取聚合结果呢？在Aggregate中，因为Aggregate是真正暴露给外部执行SQL语句调用的，Aggregate会根据聚合字段的类型来选择具体的聚合器。

```
public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
    // some code goes here
    super.open();
    this.child.open();

    TupleDesc originTd = this.child.getTupleDesc();
    // Build aggregator
    if (originTd.getFieldType(agField) == Type.INT_TYPE) {
        this.aggregator = new IntegerAggregator(this.gbField, this.gbFieldType, this.agField, this.op);
    } else {
        this.aggregator = new StringAggregator(this.gbField, this.gbFieldType, this.agField, this.op);
    }

    // Merge tuples into group
    while (this.child.hasNext()) {
        final Tuple tuple = this.child.next();
        this.aggregator.mergeTupleIntoGroup(tuple);
    }
    this.iterator = (TupleIterator) this.aggregator.iterator();
    this.iterator.open();
}
```

可以看到，open做的事很简单：不断的读取数据源，利用聚合器的mergeTupleIntoGroup进行聚合运算，当所有记录都聚合完成，返回聚合器的iterator，即聚合结果。

### StringAggregator

理解了IntegerAggregator和Aggregate的调用关系，来写StringAggregator就很容易，因为StringAggregator只支持count运算，所以相当于IntegerAggregator的简化版了，下面是mergeTupleIntoGroup的实现：

```java
public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.td == null) {
            buildTupleDesc(tup.getTupleDesc());
        }
        final Field gbField = tup.getField(this.gbField);
        final Field target = (this.gbField == NO_GROUPING ? DEFAULT_FIELD : gbField);
        this.groupMap.put(target, this.groupMap.getOrDefault(target, 0) + 1);
    }
```

可以看到基本思路和IntegerAggregator几乎一样。

## Exercise3:HeapFile Mutability

为了在 Exercise4 中实现 insert 和 delete operator, 我们需要先实现 heapFile insert / delete

讲义介绍：

> Now, we will begin to implement methods to support modifying tables. We begin at the level of individual pages and
> files. There are two main sets of operations: adding tuples and removing tuples.
>
> **Removing tuples:** To remove a tuple, you will need to implement
> `deleteTuple`. Tuples contain `RecordIDs` which allow you to find the page they reside on, so this should be as simple
> as locating the page a tuple belongs to and modifying the headers of the page appropriately.
>
> **Adding tuples:** The `insertTuple` method in
> `HeapFile.java` is responsible for adding a tuple to a heap file. To add a new tuple to a HeapFile, you will have to
> find a page with an empty slot. If no such pages exist in the HeapFile, you need to create a new page and append it to
> the physical file on disk. You will need to ensure that the RecordID in the tuple is updated correctly.

需要实现的内容：

Implement the remaining skeleton methods in:

------

- src/java/simpledb/storage/HeapPage.java

- src/java/simpledb/storage/HeapFile.java

  (Note that you do not necessarily need to implement writePage at this point).

- src/java/simpledb/storage/BufferPool.java:

    - insertTuple()
    - deleteTuple()

------

简单来说，exercise3需要我们实现HeapPage、HeapFile、BufferPool的插入元组和删除元组的方法。

### HeapPage

我们要在HeapPage中插入元组，要做的第一件事就是找空槽位然后进行插入，再处理相关细节；

我们要在HeapPage删除tuple，首先需要找到tuple在哪个slot，再进行删除即可。

插入元组的思路：找到一个空的slot，然后进行插入，并标记slot已经被使用。代码如下：

```java
   public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (!t.getTupleDesc().equals(this.td)) {
            throw new DbException("Tuple desc is not match");
        }
        for (int i = 0; i < getNumTuples(); i++) {
            if (!isSlotUsed(i)) {
                markSlotUsed(i, true);
                t.setRecordId(new RecordId(this.pid, i));
                this.tuples[i] = t;
                return;
            }
        }
        throw new DbException("The page is full");
    }

```

删除元组的思路：找到元组对应的slot，标记slot为使用，并将tuples数组对应slot的tuple置为空，具体实现代码：

```java
   public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        final RecordId recordId = t.getRecordId();
        final HeapPageId pageId = (HeapPageId) recordId.getPageId();
        final int tn = recordId.getTupleNumber();
        if (!pageId.equals(this.pid)) {
            throw new DbException("Page id not match");
        }
        if (!isSlotUsed(tn)) {
            throw new DbException("Slot is not used");
        }
        markSlotUsed(tn, false);
        this.tuples[tn] = null;
    }
```

### HeapFile

实际我们插入和删除元组，都是以HeapFile为入口的，以插入元组为例，HeapFile和HeapPage的调用关系应该是这样的：

1.调用HeapFile的insertTuple

2.HeapFile的insertTuple遍历所有数据页（用BufferPool.getPage()获取，getPage会先从BufferPool再从磁盘获取），然后判断数据页是否有空slot，有的话调用对应有空slot的page的insertTuple方法去插入页面；

3.如果遍历完所有数据页，没有找到空的slot，这时应该在磁盘中创建一个空的数据页，并且先调用 writePage() 写入该数据页到磁盘, 并通过 bufferPool 来获取该数据也

4.插入的页面保存到list中并返回，表明这是脏页，后续事务部分会用到。

HeapFile.insertTuple(Tuple tup)实现代码如下：

```java
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException,
                                                                  TransactionAbortedException {
        // some code goes here
        final ArrayList<Page> dirtyPageList = new ArrayList<>();
        for (int i = 0; i < this.numPages(); i++) {
            final HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i),
                Permissions.READ_WRITE);
            if (page != null && page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                dirtyPageList.add(page);
                break;
            }
        }
        // That means all pages are full, we should create a new page
        if (dirtyPageList.size() == 0) {
            final HeapPageId heapPageId = new HeapPageId(getId(), this.numPages());
            HeapPage newPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            writePage(newPage);
            // Through buffer pool to get newPage
            newPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            newPage.insertTuple(t);
            newPage.markDirty(true, tid);
            dirtyPageList.add(newPage);
        }
        return dirtyPageList;
    }
```

而删除更加简单，只需要根据tuple得到对应的数据页HeapPage，然后调用数据页的deleteTuple即可：

```java
    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        final ArrayList<Page> dirtyPageList = new ArrayList<>();
        final RecordId recordId = t.getRecordId();
        final PageId pageId = recordId.getPageId();
        final HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        if (page != null && page.isSlotUsed(recordId.getTupleNumber())) {
            page.deleteTuple(t);
            dirtyPageList.add(page);
        }
        return dirtyPageList;
    }
```

### BufferPool

以插入元组为例，BufferPool与HeapFile的调用关系：

1.BufferPool插入元组，会先调用Database.getCatalog().getDatabaseFile(tableId)获取HeapFile即表文件；

2.执行HeapFile.insertTuple()，插入元组并返回插入成功的页面；

3.使用HeapPage的markDirty方法，将返回的页面标记为脏页，并放入缓存池中

实现代码如下：

```java
    public void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException,
                                                                    TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        final DbFile table = Database.getCatalog().getDatabaseFile(tableId);
        final List<Page> dirtyPages = table.insertTuple(tid, t);
        for (final Page page : dirtyPages) {
            page.markDirty(true, tid);
            this.lruCache.put(page.getId(), page);
        }
    }
```

删除元组也是同样的套路：

```java
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile table = Database.getCatalog().getDatabaseFile(tableId);
        final List<Page> dirtyPages = table.deleteTuple(tid, t);
        for (final Page page : dirtyPages) {
            page.markDirty(true, tid);
            this.lruCache.put(page.getId(), page);
        }
    }

```

## Exercise4:Insertion and deletion

exercise4要求我们实现Insertion and deletion两个操作符，实际上就是两个迭代器，实现方式与exercise1相似，将传入的数据源进行处理，并返回处理结果，而处理并返回结果一般都是写在fetchNext中。这里的处理结果元组，只有一个字段，那就是插入或删除影响的行数，与MySQL相似。具体实现插入和删除，需要调用我们exercise3实现的插入删除元组相关方法。

### Insert

```
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int cnt = 0;
        while (this.child.hasNext()) {
            final Tuple next = this.child.next();
            try {
                Database.getBufferPool().insertTuple(this.tid, this.tableId, next);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error happen when insert tuple:" + e.getMessage());
            }
            cnt++;
        }
        if (cnt == 0 && isFetched) {
            return null;
        }
        isFetched = true;
        final Tuple result = new Tuple(this.td);
        result.setField(0, new IntField(cnt));
        return result;
    }
```



### Delete

类似的，Delete操作符的实现也很简单

## Exercise5: Page eviction

exercise5要求我们实现一种BufferPool的页面淘汰策略, 我在 lab1 中已经实现了.





