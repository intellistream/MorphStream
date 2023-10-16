import {AbstractRequest} from "./AbstractRequest";

export interface PerformanceRequest extends AbstractRequest {
  appId: string;
  latestBatch: number;
}
