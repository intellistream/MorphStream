import {TPGNode} from "./TPGNode";
import {OverallTimeBreakdown} from "./OverallTimeBreakdown";

export interface Batch {
  batchId: number;
  jobId: string;
  operatorID: string;
  throughput: number;
  minLatency: number;
  maxLatency: number;
  avgLatency: number;
  batchSize: number;
  batchDuration: number;
  latestBatchId: number;
  overallTimeBreakdown: OverallTimeBreakdown;
  accumulativeLatency: number;
  accumulativeThroughput: number;
  tpg: TPGNode[];
}
