export interface Operator {
  id: number;
  name: string;
  numOfInstances: number;
  throughput: number; // tuples/s
  latency: number;  // ms
  explorationStrategy: string;
  schedulingGranularity: string;
  abortHandling: string;
}
