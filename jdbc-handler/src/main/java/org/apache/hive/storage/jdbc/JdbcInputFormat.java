/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.storage.jdbc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hive.storage.jdbc.dao.DatabaseAccessor;
import org.apache.hive.storage.jdbc.dao.DatabaseAccessorFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JdbcInputFormat extends HiveInputFormat<LongWritable, MapWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcInputFormat.class);
  private DatabaseAccessor dbAccessor = null;
  private static final int SPLIT_SIZE = 75000;
  private static final double SPLIT_SLOP = 1.1;


  /**
   * {@inheritDoc}
   */
  @Override
  public RecordReader<LongWritable, MapWritable>
    getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

    if (!(split instanceof JdbcInputSplit)) {
      throw new RuntimeException("Incompatible split type " + split.getClass().getName() + ".");
    }

    return new JdbcRecordReader(job, (JdbcInputSplit) split);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    try {
      if (numSplits <= 0) {
        numSplits = 1;
      }
      LOGGER.info("Creating {} input splits", numSplits);
      dbAccessor = DatabaseAccessorFactory.getAccessor(job);

      //数据源总记录数
      long numRecords = dbAccessor.getTotalNumberOfRecords(job);
      LOGGER.info("Num records = {}", numRecords);

      Path[] tablePaths = FileInputFormat.getInputPaths(job);

      //如果数据量过少，则没必要根据numSplits切片
      if(numRecords < SPLIT_SIZE){
        InputSplit[] splits = new InputSplit[1];
        splits[0] = new JdbcInputSplit(numRecords, 0, tablePaths[0]);
        return splits;
      }

      //每75000条数据一个切片
      long remainRecords = numRecords;
      List<InputSplit> splits = new ArrayList<>();
      while (((double) remainRecords)/SPLIT_SIZE > SPLIT_SLOP){
        splits.add(new JdbcInputSplit(SPLIT_SIZE, numRecords - remainRecords, tablePaths[0]));
        remainRecords = remainRecords - SPLIT_SIZE;
      }
      //剩余的数据单独一个切片
      if(remainRecords != 0){
        splits.add(new JdbcInputSplit(remainRecords, numRecords - remainRecords, tablePaths[0]));
      }
//      //每个切片的数量
//      long numRecordsPerSplit = numRecords / numSplits;
//      long numSplitsWithExtraRecords = numRecords % numSplits;
//      InputSplit[] splits = new InputSplit[numSplits];
//
//      int offset = 0;
//      for (int i = 0; i < numSplits; i++) {
//        long numRecordsInThisSplit = numRecordsPerSplit;
//        if (i < numSplitsWithExtraRecords) {
//          numRecordsInThisSplit++;
//        }
//
//        splits[i] = new JdbcInputSplit(numRecordsInThisSplit, offset, tablePaths[0]);
//        offset += numRecordsInThisSplit;
//      }

      return splits.toArray(new InputSplit[splits.size()]);
    }
    catch (Exception e) {
      LOGGER.error("Error while splitting input data.", e);
      throw new IOException(e);
    }
  }


  /**
   * For testing purposes only
   *
   * @param dbAccessor
   *            DatabaseAccessor object
   */
  public void setDbAccessor(DatabaseAccessor dbAccessor) {
    this.dbAccessor = dbAccessor;
  }

}
