import {Operator} from "./operator";

export interface Application {
  appId: string;
  cpu: string;
  duration: string;
  name: string;
  nthreads: string;
  ram: string;
  startTime: string;
  isRunning: boolean;
  operators: Operator[];

  nevents: number;
  minProcessTime: number;
  maxProcessTime: number;
  meanProcessTime: number;
  latency: number;
  throughput: number;
  ncore: number;
}
