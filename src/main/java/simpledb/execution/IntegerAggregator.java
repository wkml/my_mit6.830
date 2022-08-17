package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 * 一个聚合器, 其对传入的 Tuple , 根据 group by 字段进行 merge 操作, 将 tuple 的值合并到之前的信息中
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static class AggInfo {
        int cnt;
        int sum;
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
    }

    // SELECT SUM(fee) AS country_group_total_fee, country FROM member GROUP BY country

    // 用于保存聚合的结果集，后面进行运算会用到
    private Map<Field, AggInfo> groupMap;
    // 依据 tuple 的第几个字段进行聚合操作,当无需分组时groupField的值为-1，在上面的SQL语句中相当于country这个字段
    private int gbField;
    // 分组字段的类型，如果无需分组这个属性值为nul
    private Type gbFieldType;
    // 对tuple的第几个字段进行聚合，在上面的SQL语句中相当于fee字段
    private int agField;
    // 进行聚合运算的操作符，相当于上述SQL语句的SUM
    private Op op;
    private Field DEFAULT_FIELD = new StringField("Default", 10);
    // 结果元组的描述信息
    private TupleDesc td;

    /**
     * Aggregate constructor
     *
     * @param gbField     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbFieldType the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param aField      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbField, Type gbFieldType, int aField, Op what) {
        // some code goes here
        this.groupMap = new HashMap<>();
        this.gbField = gbField;
        this.agField = aField;
        this.op = what;
        this.gbFieldType = gbFieldType;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.td == null) {
            buildTupleDesc(tup.getTupleDesc());
        }
        final IntField agField = (IntField) tup.getField(this.agField);
        final Field gbField = this.gbField == NO_GROUPING ? new IntField(0) : tup.getField(this.gbField);
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

    private int parseValue(final Field key) {
        if (key != null && this.groupMap.containsKey(key)) {
            AggInfo preInfo = this.groupMap.get(key);
            switch (this.op) {
                case MIN: {
                    return preInfo.min;
                }
                case MAX: {
                    return preInfo.max;
                }
                case AVG: {
                    return preInfo.sum / preInfo.cnt;
                }
                case SUM: {
                    return preInfo.sum;
                }
                case COUNT: {
                    return preInfo.cnt;
                }
            }
        }
        return 0;
    }

    public void buildTupleDesc(final TupleDesc originTd) {
        // some code goes here
        if (this.gbField == NO_GROUPING) {
            Type[] types = new Type[]{Type.INT_TYPE};
            String[] names = new String[]{""};
            this.td = new TupleDesc(types, names);
        } else {
            Type[] types = new Type[]{this.gbFieldType, Type.INT_TYPE};
            String[] names = new String[]{originTd.getFieldName(this.gbField), originTd.getFieldName(this.agField)};
            this.td = new TupleDesc(types, names);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
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
}
