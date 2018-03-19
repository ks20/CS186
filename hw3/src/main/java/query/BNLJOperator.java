package edu.berkeley.cs186.database.query;

import java.nio.ByteBuffer;
import java.util.*;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class BNLJOperator extends JoinOperator {

  protected int numBuffers;

  public BNLJOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
            rightSource,
            leftColumnName,
            rightColumnName,
            transaction,
            JoinType.BNLJ);

    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJIterator();
  }


  /**
   * BNLJ: Block Nested Loop Join
   *  See lecture slides.
   *
   * An implementation of Iterator that provides an iterator interface for this operator.
   *
   * Before proceeding, you should read and understand SNLJOperator.java
   *    You can find it in the same directory as this file.
   *
   * Word of advice: try to decompose the problem into distinguishable sub-problems.
   *    This means you'll probably want to add more methods than those given (Once again,
   *    SNLJOperator.java might prove to be a useful reference).
   */
  private class BNLJIterator extends JoinIterator {
    /**
     * Some member variables are provided for guidance, but there are many possible solutions.
     * You should implement the solution that's best for you, using any member variables you need.
     * You're free to use these member variables, but you're not obligated to.
     */

    //private Iterator<Page> leftIterator = null;
    //private Iterator<Page> rightIterator = null;
    //private BacktrackingIterator<Record> leftRecordIterator = null;
    //private BacktrackingIterator<Record> rightRecordIterator = null;

    private Iterator<Page> leftPageIter;
    private Iterator<Page> rightPageIter;
    private BacktrackingIterator<Record> leftRecordIterator;
    private BacktrackingIterator<Record> rightRecordIterator;
    private Page currentRightPage;
    private Page[] tempBlock;
    private Page[] leftBlock;
    private Page headerLeftPage;
    private Page headerRightPage;
    private Record leftRecord;
    private Record rightRecord;
    private Record nextRecord;
    private boolean flag;

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      this.leftPageIter = BNLJOperator.this.getPageIterator(this.getLeftTableName());
      this.rightPageIter = BNLJOperator.this.getPageIterator(this.getRightTableName());
      this.headerLeftPage = leftPageIter.next();
      this.headerRightPage = rightPageIter.next();

      tempBlock = new Page[numBuffers];
      populateArr();
      filterNulls(tempBlock);
      this.currentRightPage = rightPageIter.next();
      this.leftRecordIterator = BNLJOperator.this.getBlockIterator(getLeftTableName(), leftBlock);
      this.flag = false;
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

      try {
        while (true) {
          if (this.leftRecord == null) {
            if (nextLeftRecord()) {
              this.rightRecordIterator = BNLJOperator.this.getBlockIterator(getRightTableName(), new Page[]{currentRightPage});
            }
            else {
              if (this.rightPageIter.hasNext()) {
                this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), leftBlock);

                if (this.leftRecordIterator.hasNext()) {
                  this.leftRecord = this.leftRecordIterator.next();
                }
              }

              else if (!this.rightPageIter.hasNext()){
                this.leftBlock = new Page[numBuffers];
                populateArr();
                filterNulls(leftBlock);

                this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), leftBlock);
                if (nextLeftRecord()) {;}
                else { return false; }
                this.rightPageIter = BNLJOperator.this.getPageIterator(this.getRightTableName());
                this.headerRightPage = this.rightPageIter.next();
              }

              this.currentRightPage = this.rightPageIter.next();
              this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), new Page[]{currentRightPage});
            }
          }

          while (this.rightRecordIterator.hasNext()) {
            fetchNextRecord();
            if (flag) {
              flag = false;
              return true;
            }
          }
          this.leftRecord = null;
        }
      } catch (DatabaseException exception) {
        return false;
      }
    }

    private void fetchNextRecord() throws DatabaseException {
      this.rightRecord = this.rightRecordIterator.next();
      DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
      DataBox rightJoinValue = this.rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
      if (leftJoinValue.equals(rightJoinValue)) {
        List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
        List<DataBox> rightValues = new ArrayList<>(this.rightRecord.getValues());
        leftValues.addAll(rightValues);
        this.nextRecord = new Record(leftValues);
        flag = true;
      }
    }

    private boolean nextLeftRecord() {
      if (!this.leftRecordIterator.hasNext()) {
        return false;
      }
      this.leftRecord = this.leftRecordIterator.next();
      return true;
    }

    private void filterNulls(Page[] arr) {
      int count = 0;
      for (Page p: arr) {
        if (p == null) {
          count++;
        }
      }

      leftBlock = new Page[arr.length - count];
      for (int i = 0; i < leftBlock.length; i++) {
        if (arr[i] == null) {
          continue;
        }
        leftBlock[i] = arr[i];
      }
    }

    private void populateArr() {
      for (int i = 0; i < numBuffers; i++) {
        if (leftPageIter.hasNext()) {
          tempBlock[i] = leftPageIter.next();
        }
        else {
          tempBlock[i] = null;
        }
      }
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
  }
}
