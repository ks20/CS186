package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import edu.berkeley.cs186.database.table.Schema;

import javax.xml.crypto.Data;

public class SortMergeOperator extends JoinOperator {

  public SortMergeOperator(QueryOperator leftSource,
           QueryOperator rightSource,
           String leftColumnName,
           String rightColumnName,
           Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might be a useful reference).
   * 
   * Also, see discussion slides for week 7. 
   */
  private class SortMergeIterator extends JoinIterator {
    /** 
    * Some member variables are provided for guidance, but there are many possible solutions.
    * You should implement the solution that's best for you, using any member variables you need.
    * You're free to use these member variables, but you're not obligated to.
    */

    // private String leftTableName;
    // private String rightTableName;
    private RecordIterator leftIterator;
    private RecordIterator rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private Record currLeft;
    private boolean marked;
    private boolean init;
    private LR_RecordComparator lrRecordComparator;
    private LeftRecordComparator leftRecordComparator;
    private RightRecordComparator rightRecordComparator;

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      super();
      leftRecordComparator = new LeftRecordComparator();
      rightRecordComparator = new RightRecordComparator();
      lrRecordComparator = new LR_RecordComparator();

      SortOperator leftSort = new SortOperator(getTransaction(), getLeftTableName(), leftRecordComparator);
      SortOperator rightSort = new SortOperator(getTransaction(), getRightTableName(), rightRecordComparator);

      leftIterator = getRecordIterator(leftSort.sort());
      rightIterator = getRecordIterator(rightSort.sort());
      marked = true;
      init = true;
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (this.nextRecord != null) {
        return true;
      }

      if (this.leftRecord == null) {
        if (!this.leftIterator.hasNext()) {
          return false;
        }
      }

      if (this.leftRecord != null) {
        this.currLeft = this.leftRecord;
      }
      else {
        this.currLeft = this.leftIterator.next();
      }

      if (!this.marked) {
        this.rightIterator.mark();
        this.marked = true;
      }

      if (nextRightRecord()) {;}
      else {
        if (!nextLeftRecord()) {
          return false;
        }

        this.rightIterator.reset();
        return hasNext();
      }

      if (this.leftRecord == null) {
        this.leftRecord = this.currLeft;
      }

      if (this.init) {
        this.init = false;
        this.rightIterator.mark();
      }

      while (this.lrRecordComparator.compare(this.currLeft, this.rightRecord) != 0) {
        if (this.lrRecordComparator.compare(this.currLeft, this.rightRecord) < 0) {
          if (!nextLeftRecord()) {
            return false;
          }
          this.currLeft = this.leftRecord;
          resetRightRecord();
        }
        else {
          if (!nextRightRecord()) {
            return false;
          }
          this.marked = false;
        }
      }

      List<DataBox> leftValues = new ArrayList<>(this.currLeft.getValues());
      List<DataBox> rightValues = new ArrayList<>(this.rightRecord.getValues());
      leftValues.addAll(rightValues);
      this.nextRecord = new Record(leftValues);
      return true;
    }

    private void resetRightRecord(){
      this.rightIterator.reset();
      assert(rightIterator.hasNext());
      rightRecord = rightIterator.next();
      rightIterator.mark();
    }

    private boolean nextLeftRecord() {
      if (!this.leftIterator.hasNext()) {
        return false;
      }
      this.leftRecord = this.leftIterator.next();
      return true;
    }

    private boolean nextRightRecord() {
      if (!this.rightIterator.hasNext()) {
        return false;
      }
      this.rightRecord = this.rightIterator.next();
      return true;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      if (this.nextRecord == null) {
        throw new NoSuchElementException();
      }

      Record nextRec = this.nextRecord;
      this.nextRecord = null;
      return nextRec;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }


    private class LeftRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }

    /**
    * Left-Right Record comparator
    * o1 : leftRecord
    * o2: rightRecord
    */
    private class LR_RecordComparator implements Comparator<Record> {
      public int compare(Record o1, Record o2) {
        return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }
  }
}
