package intellistream.morphstream.engine.txn.operator;

import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.db.impl.SimpleDatabase;
import intellistream.morphstream.engine.db.storage.SchemaRecord;
import intellistream.morphstream.engine.db.storage.datatype.DataBox;

import java.util.*;

/**
 * QueryPlan provides a set of functions to generate simple queries. Calling the methods corresponding
 * to SQL syntax stores the information in the QueryPlan, and calling execute generates and executes
 * a QueryPlan DAG.
 */
public class QueryPlan {
    private final SimpleDatabase.Transaction transaction;
    private final String startTableName;
    private final List<String> joinTableNames;
    private final List<String> joinLeftColumnNames;
    private final List<String> joinRightColumnNames;
    private final List<String> selectColumnNames;
    private final List<PredicateOperator> selectOperators;
    private final List<DataBox> selectDataBoxes;
    private QueryOperator finalOperator;
    private List<String> projectColumns;
    private String groupByColumn;
    private String orderByColumn;
    private boolean orderByASC;
    private int orderByLimit;
    private boolean hasCount;
    private String averageColumnName;
    private String sumColumnName;

    /**
     * Creates a new QueryPlan within transaction. The base table is startTableName.
     *
     * @param transaction    the transaction containing this query
     * @param startTableName the source table for this query
     */
    public QueryPlan(SimpleDatabase.Transaction transaction, String startTableName) {
        this.transaction = transaction;
        this.startTableName = startTableName;
        this.projectColumns = new ArrayList<>();
        this.joinTableNames = new ArrayList<>();
        this.joinLeftColumnNames = new ArrayList<>();
        this.joinRightColumnNames = new ArrayList<>();
        this.selectColumnNames = new ArrayList<>();
        this.selectOperators = new ArrayList<>();
        this.selectDataBoxes = new ArrayList<>();
        this.hasCount = false;
        this.averageColumnName = null;
        this.sumColumnName = null;
        this.groupByColumn = null;
        this.orderByColumn = null;
        this.finalOperator = null;
    }

    public QueryOperator getFinalOperator() {
        return this.finalOperator;
    }

    /**
     * Add a project operator to the QueryPlan with a list of column names. Can only specify one set
     * of projections.
     *
     * @param columnNames the columns to project
     * @throws QueryPlanException
     */
    public void project(List<String> columnNames) throws QueryPlanException {
        if (!this.projectColumns.isEmpty()) {
            throw new QueryPlanException("Cannot addOperation more than one project operator to this query.");
        }
        if (columnNames.isEmpty()) {
            throw new QueryPlanException("Cannot project no columns.");
        }
        this.projectColumns = columnNames;
    }

    /**
     * Add a select operator. Only returns columns in which the column fulfills the predicate relative
     * to value_list.
     *
     * @param column     the column to specify the predicate on
     * @param comparison the comparator
     * @param value      the value_list to compare against
     * @throws QueryPlanException
     */
    public void select(String column, PredicateOperator comparison, DataBox value) {
        this.selectColumnNames.add(column);
        this.selectOperators.add(comparison);
        this.selectDataBoxes.add(value);
    }

    /**
     * Set the group by column for this query.
     *
     * @param column the column to group by
     * @throws QueryPlanException
     */
    public void groupBy(String column) {
        this.groupByColumn = column;
    }

    /**
     * Set the order by column for this query.
     *
     * @param column
     * @param ASC
     * @param limit
     */
    public void orderby(String column, boolean ASC, int limit) {
        this.orderByColumn = column;
        this.orderByASC = ASC;
        this.orderByLimit = limit;
    }

    /**
     * Add a count aggregate to this query. Only can specify count(*).
     *
     * @throws QueryPlanException
     */
    public void count() {
        this.hasCount = true;
    }

    /**
     * Add an average on column. Can only average over integer or float columns.
     *
     * @param column the column to average
     * @throws QueryPlanException
     */
    public void average(String column) {
        this.averageColumnName = column;
    }

    /**
     * Add a sum on column. Can only sum integer or float columns
     *
     * @param column the column to sum
     * @throws QueryPlanException
     */
    public void sum(String column) {
        this.sumColumnName = column;
    }

    /**
     * Join the leftColumnName column of the existing queryplan against the rightColumnName column
     * of tableName.
     *
     * @param tableName       the table to join against
     * @param leftColumnName  the join column in the existing QueryPlan
     * @param rightColumnName the join column in tableName
     */
    public void join(String tableName, String leftColumnName, String rightColumnName) {
        this.joinTableNames.add(tableName);
        this.joinLeftColumnNames.add(leftColumnName);
        this.joinRightColumnNames.add(rightColumnName);
    }

    /**
     * Generates a naïve QueryPlan in which all joins are at the bottom of the DAG followed by all select
     * predicates, an optional group by operator, and a set of projects (in that order).
     *
     * @return an iterator of records that is the result of this query
     * @throws DatabaseException
     * @throws QueryPlanException
     */
    public Iterator<SchemaRecord> execute() throws DatabaseException, QueryPlanException {
        String indexColumn = this.checkIndexEligible();
        if (indexColumn != null) {
            this.generateIndexPlan(indexColumn);
        } else {
            // start off with the start table scan as the source
            this.finalOperator = new SequentialScanOperator(this.transaction, this.startTableName);
            this.addJoins();
            this.addSelects();
            this.addGroupBy();
            this.addProjects();
        }
        return this.finalOperator.execute();
    }

    /**
     * Generates an optimal QueryPlan based on the System R cost-based query txn.
     *
     * @return an iterator of records that is the result of this query
     * @throws DatabaseException
     * @throws QueryPlanException
     */
    public Iterator<SchemaRecord> executeOptimal() throws DatabaseException, QueryPlanException {
        List<String> tableNames = new ArrayList<>();
        tableNames.add(this.startTableName);
        tableNames.addAll(this.joinTableNames);
        int pass = 1;
        // Pass 1: Iterate through all partition tables. For each partition table, find
        // the lowest cost QueryOperator to access that table. Construct a mapping
        // of each table name to its lowest cost operator.
        Map<Set, QueryOperator> map = new HashMap<>();
        for (String table : tableNames) {
            QueryOperator minOp = this.minCostSingleAccess(table);
            Set<String> key = new HashSet<>();
            key.add(table);
            map.put(key, minOp);
        }
        // Pass i: On each pass, use the results from the previous pass to find the
        // lowest cost joins with each partition table. Repeat until all tables have
        // been joined.
        Map<Set, QueryOperator> pass1Map = map;
        Map<Set, QueryOperator> prevMap;
        while (pass++ < tableNames.size()) {
            prevMap = map;
            map = this.minCostJoins(prevMap, pass1Map);
        }
        // Get the lowest cost operator from the last pass, addOperation GROUP BY and SELECT
        // operators, and return an iterator on the final operator
        this.finalOperator = this.minCostOperator(map);
        this.addGroupBy();
        this.addProjects();
        return this.finalOperator.iterator();
    }

    /**
     * Gets all SELECT predicates for which there exists an index on the column
     * referenced in that predicate for the given table.
     *
     * @return an ArrayList of SELECT predicates
     */
    private List<Integer> getEligibleIndexColumns(String table) {
        List<Integer> selectIndices = new ArrayList<>();
        for (int i = 0; i < this.selectColumnNames.size(); i++) {
            String column = this.selectColumnNames.get(i);
            if (this.transaction.indexExists(table, column) &&
                    this.selectOperators.get(i) != PredicateOperator.NOT_EQUALS) {
                selectIndices.add(i);
            }
        }
        return selectIndices;
    }

    /**
     * Applies all eligible SELECT predicates to a given source, except for the
     * predicate at index except. The purpose of except is because there might
     * be one SELECT predicate that was already used for an index scan, so no
     * point applying it again. A SELECT predicate is represented as elements of
     * this.selectColumnNames, this.selectOperators, and this.selectDataBoxes that
     * correspond to the same index of these lists.
     *
     * @return a new QueryOperator after SELECT has been applied
     * @throws DatabaseException
     * @throws QueryPlanException
     */
    private QueryOperator pushDownSelects(QueryOperator source, int except) {
        /* TODO: Implement me! */
        this.finalOperator = source;
        if (except != -1) {
            List<Integer> list = getEligibleIndexColumns(this.startTableName);
            int index = 0;
            for (Integer i : list) {
                if (i == except) {
                    index++;
                    continue;
                }
                try {
                    PredicateOperator operator = this.selectOperators.get(index);
                    String column = this.selectColumnNames.get(index);
                    DataBox value = this.selectDataBoxes.get(index);
                    SelectOperator selectOperator = new SelectOperator(this.finalOperator, column, operator, value);
                    this.finalOperator = selectOperator;
                    index++;
                } catch (QueryPlanException e) {
                    index++;
                }
            }
        } else {
            addSelects();
        }
        return this.finalOperator;
    }

    /**
     * Finds the lowest cost QueryOperator that scans the given table. First
     * determine the cost of a sequential scan. Then for every index that can be
     * used on that table, determine the cost of an index scan. Keep track of
     * the minimum cost Operation. Then push down eligible projects (SELECT
     * predicates). If an index scan was chosen, exclude that SELECT predicate from
     * the push down. This method is called during the first pass of the search
     * algorithm to determine the most efficient way to access each partition table.
     *
     * @return a QueryOperator that scans the given table
     * @throws DatabaseException
     * @throws QueryPlanException
     */
    private QueryOperator minCostSingleAccess(String table) throws QueryPlanException {
        QueryOperator minOp = null;
        // Find the cost of a sequential scan of the table
        // TODO: Implement me!
        QueryOperator sequentialScan = new SequentialScanOperator(this.transaction, table);
        int sequentialScanIO = sequentialScan.getIOCost();
        minOp = sequentialScan;
        // For each eligible index column, find the cost of an index scan of the
        // table and retain the lowest cost operator
        // TODO: Implement me!
        List<Integer> selectIndices = this.getEligibleIndexColumns(table);
        int minSelectIdx = -1;
        if (selectIndices.size() != 0) {
            int smallest = Integer.MAX_VALUE;
            for (Integer i : selectIndices) {
                String column = this.selectColumnNames.get(i);
                PredicateOperator operator = this.selectOperators.get(i);
                DataBox value = this.selectDataBoxes.get(i);
                QueryOperator indexOp = new IndexScanOperator(this.transaction, table, column, operator, value);
                int tempIO = indexOp.getIOCost();
                if (tempIO < smallest) {
                    smallest = tempIO;
                    minSelectIdx = i;
                    minOp = indexOp;
                }
            }
            if (sequentialScanIO <= minOp.getIOCost()) {
                minOp = sequentialScan;
                minSelectIdx = -1;
            }
        }
        // Push down SELECT predicates that apply to this table and that were not
        // used for an index scan
        minOp = this.pushDownSelects(minOp, minSelectIdx);
        return minOp;
    }

    /**
     * Given a join condition between an outer relation represented by leftOp
     * and an inner relation represented by rightOp, find the lowest cost join
     * operator out of all the possible join types in JoinOperator.JoinType.
     *
     * @return lowest cost join QueryOperator between the input operators
     * @throws QueryPlanException
     */
    private QueryOperator minCostJoinType(QueryOperator leftOp,
                                          QueryOperator rightOp,
                                          String leftColumn,
                                          String rightColumn) throws QueryPlanException {
        QueryOperator minOp = null;
        /* TODO: Implement me! */
        int smallest = Integer.MAX_VALUE;
        for (JoinOperator.JoinType join : JoinOperator.JoinType.values()) {
            JoinOperator op = null;
            switch (join) {
                case SNLJ:
                    op = new SNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction);
                    break;
//				case PNLJ:
//					op = new PNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction);
//					break;
//				case BNLJ:
//					op = new BNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction);
//					break;
                case GRACEHASH:
                    op = new GraceHashOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction);
                    break;
                case SORTMERGE:
                    //op = new SortMergeOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction);
                    break;
            }
            if (op != null) {
                if (op.getIOCost() < smallest) {
                    smallest = op.getIOCost();
                    minOp = op;
                }
            }
        }
        return minOp;
    }

    /**
     * Iterate through all table sets in the previous pass of the search. For each
     * table set, check each join predicate to see if there is a valid join
     * condition with a new table. If so, check the cost of each type of join and
     * keep the minimum cost join. Construct and return a mapping of each set of
     * table names being joined to its lowest cost join operator. A join predicate
     * is represented as elements of this.joinTableNames, this.joinLeftColumnNames,
     * and this.joinRightColumnNames that correspond to the same index of these lists.
     *
     * @return a mapping of table names to a join QueryOperator
     * @throws QueryPlanException
     */
    private Map<Set, QueryOperator> minCostJoins(Map<Set, QueryOperator> prevMap,
                                                 Map<Set, QueryOperator> pass1Map) throws QueryPlanException {
        Map<Set, QueryOperator> map = new HashMap<>();
        /* TODO: Implement me! */
        Set<Set> prevKeys = prevMap.keySet();
        for (Set prevKey : prevKeys) {
            for (int index = 0; index < this.joinTableNames.size(); index++) {
                String leftColumn = this.joinLeftColumnNames.get(index);
                String leftTable = leftColumn.split("\\.")[0];
                String rightColumn = this.joinRightColumnNames.get(index);
                String rightTable = this.joinTableNames.get(index);
                if (prevKey.contains(leftTable) && !prevKey.contains(rightTable)) {
                    QueryOperator leftOperator = prevMap.get(prevKey);
                    Set<String> rightSet = new HashSet<>();
                    rightSet.add(rightTable);
                    QueryOperator rightOperator = pass1Map.get(rightSet);
                    String leftCol = leftColumn.split("\\.")[1];
                    String rightCol = rightColumn.split("\\.")[1];
                    QueryOperator minOp = minCostJoinType(leftOperator, rightOperator, leftCol, rightCol);
                    Set<String> newTable = new HashSet<String>(prevKey);
                    newTable.add(rightTable);
                    map.put(newTable, minOp);
                }
            }
        }
        return map;
    }

    /**
     * Finds the lowest cost QueryOperator in the given mapping. A mapping is
     * generated on each pass of the search algorithm, and relates a set of tables
     * to the lowest cost QueryOperator accessing those tables. This method is
     * called at the end of the search algorithm after all passes have been
     * processed.
     *
     * @return a QueryOperator in the given mapping
     * @throws QueryPlanException
     */
    private QueryOperator minCostOperator(Map<Set, QueryOperator> map) {
        QueryOperator minOp = null;
        QueryOperator newOp;
        int minCost = Integer.MAX_VALUE;
        int newCost;
        for (Set tables : map.keySet()) {
            newOp = map.get(tables);
            newCost = newOp.getIOCost();
            if (newCost < minCost) {
                minOp = newOp;
                minCost = newCost;
            }
        }
        return minOp;
    }

    private String checkIndexEligible() {
        if (this.selectColumnNames.size() > 0
                && this.groupByColumn == null
                && this.orderByColumn == null
                && this.joinTableNames.size() == 0) {
            int index = 0;
            for (String column : selectColumnNames) {
                if (this.transaction.indexExists(this.startTableName, column)) {
                    if (this.selectOperators.get(index) != PredicateOperator.NOT_EQUALS) {
                        return column;
                    }
                }
                index++;
            }
        }
        return null;
    }

    private void generateIndexPlan(String indexColumn) throws QueryPlanException {
        int selectIndex = this.selectColumnNames.indexOf(indexColumn);
        PredicateOperator operator = this.selectOperators.get(selectIndex);
        DataBox value = this.selectDataBoxes.get(selectIndex);
        this.finalOperator = new IndexScanOperator(this.transaction, this.startTableName, indexColumn, operator,
                value);
        this.selectColumnNames.remove(selectIndex);
        this.selectOperators.remove(selectIndex);
        this.selectDataBoxes.remove(selectIndex);
        this.addSelects();
        this.addProjects();
    }

    private void addJoins() throws QueryPlanException {
        int index = 0;
        for (String joinTable : this.joinTableNames) {
            SequentialScanOperator scanOperator = new SequentialScanOperator(this.transaction, joinTable);
            SNLJOperator joinOperator = new SNLJOperator(finalOperator, scanOperator,
                    this.joinLeftColumnNames.get(index), this.joinRightColumnNames.get(index), this.transaction); //changed from new JoinOperator
            this.finalOperator = joinOperator;
            index++;
        }
    }

    private void addSelects() {
        int index = 0;
        for (String selectColumn : this.selectColumnNames) {
            try {
                PredicateOperator operator = this.selectOperators.get(index);
                DataBox value = this.selectDataBoxes.get(index);
                SelectOperator selectOperator = new SelectOperator(this.finalOperator, selectColumn,
                        operator, value);
                this.finalOperator = selectOperator;
                index++;
            } catch (QueryPlanException e) {
                index++;
            }
        }
    }

    private void addGroupBy() throws QueryPlanException {
        if (this.groupByColumn != null) {
            if (this.projectColumns.size() > 2 || (this.projectColumns.size() == 1 &&
                    !this.projectColumns.get(0).equals(this.groupByColumn))) {
                throw new QueryPlanException("Can only project columns specified in the GROUP BY clause.");
            }
            GroupByOperator groupByOperator = new GroupByOperator(this.finalOperator, this.transaction,
                    this.groupByColumn);
            this.finalOperator = groupByOperator;
        }
    }

    private void addOrderBy() throws QueryPlanException {
        if (this.orderByColumn != null) {
            OrderByOperator orderByOperator = new OrderByOperator(this.finalOperator, this.transaction,
                    this.orderByColumn);
            this.finalOperator = orderByOperator;
        }
    }

    private void addProjects() throws QueryPlanException {
        if (!this.projectColumns.isEmpty() || this.hasCount || this.sumColumnName != null
                || this.averageColumnName != null) {
            ProjectOperator projectOperator = new ProjectOperator(this.finalOperator, this.projectColumns,
                    this.hasCount, this.averageColumnName, this.sumColumnName);
            this.finalOperator = projectOperator;
        }
    }

    public enum PredicateOperator {
        EQUALS,
        NOT_EQUALS,
        LESS_THAN,
        LESS_THAN_EQUALS,
        GREATER_THAN,
        GREATER_THAN_EQUALS
    }
}