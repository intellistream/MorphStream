import {Operator} from "./operator";

export interface Application {
  id: number;
  name: string;
  numOfThreads: number;
  cpu: string;
  ram: string;
  startTime: string;
  duration: string;
  isRunning: boolean;
  operators: Operator[];
}
