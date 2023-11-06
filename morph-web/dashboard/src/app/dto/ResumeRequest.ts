import {AbstractRequest} from "./AbstractRequest";

export interface ResumeRequest extends AbstractRequest {
  appId: string;
  signal: string;
}
