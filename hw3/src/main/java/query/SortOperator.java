package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.io.Page;

import java.util.*;


public class SortOperator  {
  private Database.Transaction transaction;
  private String tableName;
  private Comparator<Record> comparator;
  private Schema operatorSchema;
  private int numBuffers;

  public SortOperator(Database.Transaction transaction, String tableName, Comparator<Record> comparator) throws DatabaseException, QueryPlanException {
    this.transaction = transaction;
    this.tableName = tableName;
    this.comparator = comparator;
    this.operatorSchema = this.computeSchema();
    this.numBuffers = this.transaction.getNumMemoryPages();
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }


  public class Run {
    private String tempTableName;

    public Run() throws DatabaseException {
      this.tempTableName = SortOperator.this.transaction.createTempTable(SortOperator.this.operatorSchema);
    }

    public void addRecord(List<DataBox> values) throws DatabaseException {
      SortOperator.this.transaction.addRecord(this.tempTableName, values);
    }

    public void addRecords(List<Record> records) throws DatabaseException {
      for (Record r: records) {
        this.addRecord(r.getValues());
      }
    }

    public Iterator<Record> iterator() throws DatabaseException {
      return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
    }

    public String tableName() {
      return this.tempTableName;
    }
  }


  /**
   * Returns a NEW run that is the sorted version of the input run.
   * Can do an in memory sort over all the records in this run
   * using one of Java's built-in sorting methods.
   * Note: Don't worry about modifying the original run.
   * Returning a new run would bring one extra page in memory beyond the
   * size of the buffer, but it is done this way for ease.
   */
  public Run sortRun(Run run) throws DatabaseException {
    Run newRun = new Run();

    List<Record> allRecords = new ArrayList<>();
    Iterator<Record> newIter = run.iterator();
    while (newIter.hasNext()) {
      allRecords.add(newIter.next());
    }

    allRecords.sort(comparator);
    newRun.addRecords(allRecords);

    return newRun;
  }

  /**
   * Given a list of sorted runs, returns a new run that is the result
   * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
   * to determine which record should be should be added to the output run next.
   * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
   * where a Pair (r, i) is the Record r with the smallest value you are
   * sorting on currently unmerged from run i.
   */
  public Run mergeSortedRuns(List<Run> runs) throws DatabaseException {
    Queue<Pair<Record, Integer>> pQueue = new PriorityQueue<>(new RecordPairComparator());
    List<Iterator<Record>> runIter = new ArrayList<>();
    int curr = 0;

    for (Run run : runs) {
      runIter.add(curr, run.iterator());

      if (runIter.get(curr).hasNext()) {
        Pair<Record, Integer> newPair = new Pair<>(runIter.get(curr).next(), curr);
        pQueue.add(newPair);
        curr = curr + 1;
      }
    }

    Run newRun = new Run();
    while (pQueue.size() > 0) {
      Pair<Record, Integer> next = pQueue.remove();
      List<DataBox> getVals = next.getFirst().getValues();
      newRun.addRecord(getVals);

      Integer val = next.getSecond();
      Iterator<Record> recIter = runIter.get(val);
      if (recIter.hasNext()) {
        pQueue.add(new Pair<>(recIter.next(), val));
      }
    }

    return newRun;
  }

  /**
   * Given a list of N sorted runs, returns a list of
   * sorted runs that is the result of merging (numBuffers - 1)
   * of the input runs at a time.
   */
  public List<Run> mergePass(List<Run> runs) throws DatabaseException {
    List<Run> sortedRuns = new ArrayList<>();
    int mergeBuffers = numBuffers - 1;

    int incr = 0;
    Run mergeRun;
    while (incr < runs.size()) {
      mergeRun = mergeSortedRuns(runs.subList(incr, incr + mergeBuffers));
      sortedRuns.add(mergeRun);
      incr = incr + mergeBuffers;
    }

    return sortedRuns;
  }


  /**
   * Does an external merge sort on the table with name tableName
   * using numBuffers.
   * Returns the name of the table that backs the final run.
   */
  public String sort() throws DatabaseException {
    BacktrackingIterator<Page> pIter = this.transaction.getPageIterator(this.tableName);
    pIter.next();
    List<Run> sortRuns = new ArrayList<>();
    int numBufs = numBuffers - 1;

    while (pIter.hasNext()) {
      Iterator<Record> bIter = this.transaction.getBlockIterator(this.tableName, pIter, numBufs);
      List<Record> recordList = new ArrayList<>();

      while (bIter.hasNext()) {
        recordList.add(bIter.next());
      }

      Run uRun = createRun();
      uRun.addRecords(recordList);
      Run sRun = sortRun(uRun);
      sortRuns.add(sRun);
    }

    while (sortRuns.size() > 1) {
      sortRuns = mergePass(sortRuns);
    }

    String tableFinalRun = sortRuns.get(0).tableName();
    return tableFinalRun;
  }


  private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
    public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
      return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());

    }
  }

  public Run createRun() throws DatabaseException {
    return new Run();
  }



}
