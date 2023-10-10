import { Injectable } from '@angular/core';
import {Websocket} from "../../services/utils/websocket";
import {Observable} from "rxjs";
import {BasicApplication} from "../../model/BasicApplication";
import {BasicInfoRequest} from "../../dto/BasicInfoRequest";

@Injectable({
  providedIn: 'root'
})
export class OverviewService {
  constructor(private websocket: Websocket) {
    this.websocket.connect("ws://localhost:5001/websocket");
  }

  public getAllHistoricalJobs(): Observable<BasicApplication[]> {
    let msg: BasicInfoRequest = {
      "type": "BasicInfoRequest",
      "appId": "0",
      "correlationId": ""
    }
    return this.websocket.sendRequest<BasicApplication[]>(msg);
  }
}
