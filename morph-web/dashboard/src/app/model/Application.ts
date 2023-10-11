import {Operator} from "./Operator";
import {TotalTimeBreakdown} from "./TotalTimeBreakdown";
import {SchedulerTimeBreakdown} from "./SchedulerTimeBreakdown";

export interface Application {
  appId: string;
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
  schedulerTimeBreakdown: SchedulerTimeBreakdown;
  periodicalThroughput: number[];
  periodicalLatency: number[];
}
