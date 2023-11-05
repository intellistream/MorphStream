import {Operator} from "./Operator";
import {TotalTimeBreakdown} from "./TotalTimeBreakdown";
import {OverallTimeBreakdown} from "./OverallTimeBreakdown";

export interface Job {
  jobId: string;
  name: string;
  nthreads: string;
  cpu: string;
  ram: string;
  startTime: string;
  duration: string;
  isRunning: boolean;
  nevents: number;
  minProcessTime: number;
  maxProcessTime: number;
  meanProcessTime: number;
  latency: number;
  throughput: number;
  ncore: number;
  operators: Operator[];
  totalTimeBreakdown: TotalTimeBreakdown;
  schedulerTimeBreakdown: OverallTimeBreakdown;
  periodicalThroughput: number[];
  periodicalLatency: number[];
}
