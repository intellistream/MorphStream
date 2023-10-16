import {OverallTimeBreakdown} from "./OverallTimeBreakdown";
import {TPGNode} from "./TPGNode";

export interface Performance {
  appId: string
  operatorID: string
  throughput: number
  minLatency: number
  maxLatency: number
  avgLatency: number
  batchSize: number
  batchDuration: number
  overallTimeBreakdown: OverallTimeBreakdown
  tpg: TPGNode[]
}
