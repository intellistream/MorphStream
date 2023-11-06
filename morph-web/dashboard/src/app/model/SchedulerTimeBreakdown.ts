import {OverallTimeBreakdown} from "./OverallTimeBreakdown";
import {TPGNode} from "./TPGNode";

export interface SchedulerTimeBreakdown {
  exploreTime: number,
  usefulTime: number,
  abortTime: number,
  constructTime: number,
  trackingTime: number
}
