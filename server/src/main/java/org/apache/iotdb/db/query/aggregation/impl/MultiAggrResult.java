package org.apache.iotdb.db.query.aggregation.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class MultiAggrResult {
  private TSDataType seriesDataType;
  private AvgAggrResult avgAggrResult;
  private CountAggrResult countAggrResult;
  private FirstValueAggrResult firstValueAggrResult;
  private LastValueAggrResult lastValueAggrResult;
  private MaxTimeAggrResult maxTimeAggrResult;
  private MaxValueAggrResult maxValueAggrResult;
  private MinTimeAggrResult minTimeAggrResult;
  private MinValueAggrResult minValueAggrResult;
  private SumAggrResult sumAggrResult;
  private List<AggregateResult> aggregateResultList;

  public MultiAggrResult(TSDataType seriesDataType) {
    this.seriesDataType = seriesDataType;
    this.avgAggrResult = new AvgAggrResult(seriesDataType);
    this.countAggrResult = new CountAggrResult();
    this.firstValueAggrResult = new FirstValueAggrResult(seriesDataType);
    this.lastValueAggrResult = new LastValueAggrResult(seriesDataType);
    this.maxTimeAggrResult = new MaxTimeAggrResult();
    this.maxValueAggrResult = new MaxValueAggrResult(seriesDataType);
    this.minTimeAggrResult = new MinTimeAggrResult();
    this.minValueAggrResult = new MinValueAggrResult(seriesDataType);
    this.sumAggrResult = new SumAggrResult(seriesDataType);
  }

  public List<AggregateResult>  getAggregateResultList(){
    return new ArrayList<>();
  }
  public int updateResultFromStatistics(Statistics statistics, boolean[] isCalculatedArray, int remainingToCalculate, List<String> aggregateTypeList) {
    int newRemainingToCalculate = remainingToCalculate;
    System.out.println("wolaileya");
    for (int i = 0; i < aggregateTypeList.size(); i++) {
      if (!isCalculatedArray[i]) {
        switch (aggregateTypeList.get(i).toLowerCase()){
          case SQLConstant.MIN_TIME: {
            minTimeAggrResult.updateResultFromStatistics(statistics);
            if (minTimeAggrResult.isCalculatedAggregationResult()) {
              aggregateResultList.add(minTimeAggrResult);
              isCalculatedArray[i] = true;
              newRemainingToCalculate--;
              if (newRemainingToCalculate == 0) {
                return newRemainingToCalculate;
              }
            }
            break;
          }
          case SQLConstant.MAX_TIME: {
            maxTimeAggrResult.updateResultFromStatistics(statistics);
            if (maxTimeAggrResult.isCalculatedAggregationResult()) {
              aggregateResultList.add(maxTimeAggrResult);
              isCalculatedArray[i] = true;
              newRemainingToCalculate--;
              if (newRemainingToCalculate == 0) {
                return newRemainingToCalculate;
              }
            }
            break;
          }
          case SQLConstant.MIN_VALUE: {
            minValueAggrResult.updateResultFromStatistics(statistics);
            if (minValueAggrResult.isCalculatedAggregationResult()) {
              aggregateResultList.add(minValueAggrResult);
              isCalculatedArray[i] = true;
              newRemainingToCalculate--;
              if (newRemainingToCalculate == 0) {
                return newRemainingToCalculate;
              }
            }
            break;
          }
          case SQLConstant.MAX_VALUE: {
            maxValueAggrResult.updateResultFromStatistics(statistics);
            if (maxValueAggrResult.isCalculatedAggregationResult()) {
              aggregateResultList.add(maxValueAggrResult);
              isCalculatedArray[i] = true;
              newRemainingToCalculate--;
              if (newRemainingToCalculate == 0) {
                return newRemainingToCalculate;
              }
            }
            break;
          }
          case SQLConstant.COUNT: {
            countAggrResult.updateResultFromStatistics(statistics);
            if (countAggrResult.isCalculatedAggregationResult()) {
              aggregateResultList.add(countAggrResult);
              isCalculatedArray[i] = true;
              newRemainingToCalculate--;
              if (newRemainingToCalculate == 0) {
                return newRemainingToCalculate;
              }
            }
            break;
          }
          case SQLConstant.AVG: {
            avgAggrResult.updateResultFromStatistics(statistics);
            if (avgAggrResult.isCalculatedAggregationResult()) {
              aggregateResultList.add(avgAggrResult);
              isCalculatedArray[i] = true;
              newRemainingToCalculate--;
              if (newRemainingToCalculate == 0) {
                return newRemainingToCalculate;
              }
            }
            break;
          }
          case SQLConstant.FIRST_VALUE: {
            firstValueAggrResult.updateResultFromStatistics(statistics);
            if (firstValueAggrResult.isCalculatedAggregationResult()) {
              aggregateResultList.add(firstValueAggrResult);
              isCalculatedArray[i] = true;
              newRemainingToCalculate--;
              if (newRemainingToCalculate == 0) {
                return newRemainingToCalculate;
              }
            }
            break;
          }
          case SQLConstant.SUM: {
            sumAggrResult.updateResultFromStatistics(statistics);
            if (sumAggrResult.isCalculatedAggregationResult()) {
              aggregateResultList.add(sumAggrResult);
              isCalculatedArray[i] = true;
              newRemainingToCalculate--;
              if (newRemainingToCalculate == 0) {
                return newRemainingToCalculate;
              }
            }
            break;
          }
          case SQLConstant.LAST_VALUE: {
            lastValueAggrResult.updateResultFromStatistics(statistics);
            if (lastValueAggrResult.isCalculatedAggregationResult()) {
              aggregateResultList.add(lastValueAggrResult);
              isCalculatedArray[i] = true;
              newRemainingToCalculate--;
              if (newRemainingToCalculate == 0) {
                return newRemainingToCalculate;
              }
            }
            break;
          }
          default:
            throw new IllegalArgumentException("Invalid Aggregation function: " + aggregateTypeList.get(i));
        }
      }
    }
    return newRemainingToCalculate;
  }

  public int updateResultFromPageData(BatchData dataInThisPage, List<String> aggregateTypeList, boolean[] isCalculatedArray, int remainingToCalculate)
      throws IOException {
    System.out.println("wolaile");
    while (dataInThisPage.hasCurrent()) {
      for (int i = 0; i < aggregateTypeList.size(); i++) {
        if (!isCalculatedArray[i]) {
          switch (aggregateTypeList.get(i).toLowerCase()) {
            case SQLConstant.MIN_TIME: {
              /*minTimeAggrResult.updateResultFromStatistics(statistics);*/

              if (minTimeAggrResult.isCalculatedAggregationResult()) {
                aggregateResultList.add(minTimeAggrResult);
                isCalculatedArray[i] = true;
                remainingToCalculate--;
                if (remainingToCalculate == 0) {
                  return remainingToCalculate;
                }
              }
              break;
            }
            case SQLConstant.MAX_TIME: {
              /*maxTimeAggrResult.updateResultFromStatistics(statistics);*/
              if (maxTimeAggrResult.isCalculatedAggregationResult()) {
                aggregateResultList.add(maxTimeAggrResult);
                isCalculatedArray[i] = true;
                remainingToCalculate--;
                if (remainingToCalculate == 0) {
                  return remainingToCalculate;
                }
              }
              break;
            }
            case SQLConstant.MIN_VALUE: {
              /*minValueAggrResult.updateResultFromStatistics(statistics);*/
              if (minValueAggrResult.isCalculatedAggregationResult()) {
                aggregateResultList.add(minValueAggrResult);
                isCalculatedArray[i] = true;
                remainingToCalculate--;
                if (remainingToCalculate == 0) {
                  return remainingToCalculate;
                }
              }
              break;
            }
            case SQLConstant.MAX_VALUE: {
              /*maxValueAggrResult.updateResultFromStatistics(statistics);*/
              if (maxValueAggrResult.isCalculatedAggregationResult()) {
                aggregateResultList.add(maxValueAggrResult);
                isCalculatedArray[i] = true;
                remainingToCalculate--;
                if (remainingToCalculate == 0) {
                  return remainingToCalculate;
                }
              }
              break;
            }
            case SQLConstant.COUNT: {
              /*countAggrResult.updateResultFromStatistics(statistics);*/
              if (countAggrResult.isCalculatedAggregationResult()) {
                aggregateResultList.add(countAggrResult);
                isCalculatedArray[i] = true;
                remainingToCalculate--;
                if (remainingToCalculate == 0) {
                  return remainingToCalculate;
                }
              }
              break;
            }
            case SQLConstant.AVG: {
             /* avgAggrResult.updateResultFromStatistics(statistics);*/
              avgAggrResult.updateAvg(seriesDataType, dataInThisPage.currentValue());
              if (avgAggrResult.isCalculatedAggregationResult()) {
                aggregateResultList.add(avgAggrResult);
                isCalculatedArray[i] = true;
                remainingToCalculate--;
                if (remainingToCalculate == 0) {
                  return remainingToCalculate;
                }
              }
              break;
            }
            case SQLConstant.FIRST_VALUE: {
              /*firstValueAggrResult.updateResultFromStatistics(statistics);*/
              if (firstValueAggrResult.isCalculatedAggregationResult()) {
                aggregateResultList.add(firstValueAggrResult);
                isCalculatedArray[i] = true;
                remainingToCalculate--;
                if (remainingToCalculate == 0) {
                  return remainingToCalculate;
                }
              }
              break;
            }
            case SQLConstant.SUM: {
              /*sumAggrResult.updateResultFromStatistics(statistics);*/
              if (sumAggrResult.isCalculatedAggregationResult()) {
                aggregateResultList.add(sumAggrResult);
                isCalculatedArray[i] = true;
                remainingToCalculate--;
                if (remainingToCalculate == 0) {
                  return remainingToCalculate;
                }
              }
              break;
            }
            case SQLConstant.LAST_VALUE: {
              /*lastValueAggrResult.updateResultFromStatistics(statistics);*/
              if (lastValueAggrResult.isCalculatedAggregationResult()) {
                aggregateResultList.add(lastValueAggrResult);
                isCalculatedArray[i] = true;
                remainingToCalculate--;
                if (remainingToCalculate == 0) {
                  return remainingToCalculate;
                }
              }
              break;
            }
            default:
              throw new IllegalArgumentException(
                  "Invalid Aggregation function: " + aggregateTypeList.get(i));
          }
        }
      }

    }
    return 0;
  }
}
