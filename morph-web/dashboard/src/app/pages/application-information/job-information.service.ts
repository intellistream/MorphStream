import { Injectable } from '@angular/core';
import {Websocket} from "../../services/utils/websocket";
import {Observable} from "rxjs";
import {Application} from "../../model/Application";
import {DetailedInfoRequest} from "../../dto/DetailedInfoRequest";
import {ResumeRequest} from "../../dto/ResumeRequest";
import {SignalResponse} from "../../model/SignalResponse";
import {Performance} from "../../model/Performance";
import {PerformanceRequest} from "../../dto/PerformanceRequest";

@Injectable({
  providedIn: 'root'
})
export class JobInformationService {
  constructor(private websocket: Websocket) {
    this.websocket.connect("ws://localhost:5001/websocket");
  }

  public getHistoricalJob(appId: string): Observable<Application> {
    let msg: DetailedInfoRequest = {
      "type": "DetailInfoRequest",
      "appId": appId,
      "correlationId": ""
    }

    return this.websocket.sendRequest<Application>(msg);
  }

  // public listenOnPerformanceData(jobId: string): Observable<any> {
  //   return this.websocket.listenOnJobData(jobId);
  // }


  public listenOnPerformanceData(jobId: string): Observable<Performance> {
    return this.websocket.listenOnPerformanceData(jobId);
  }

  public sendPerformanceRequest(jobId: string, operator: string, latestBatch: number) {
    let msg: PerformanceRequest = {
      "type": "Performance",
      "appId": jobId,
      "correlationId": "",
      "operator": operator, // sl
      "latestBatch": latestBatch
    }
    this.websocket.sendPerformanceRequest(msg)
    return
  }

  public sendResumeSignal(appId: string): Observable<SignalResponse> {
    let msg: ResumeRequest = {
      "type": "Signal",
      "appId": appId,
      "correlationId": "",
      "signal": "start"
    }

    return this.websocket.sendRequest<SignalResponse>(msg);
  }
}
