import {AbstractRequest} from "./AbstractRequest";

export interface BasicInfoRequest extends AbstractRequest {
  type: string;
  appId: string;
}
