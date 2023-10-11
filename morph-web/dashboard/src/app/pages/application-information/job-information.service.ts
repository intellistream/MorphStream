import { Injectable } from '@angular/core';
import {Websocket} from "../../services/utils/websocket";
import {Observable} from "rxjs";
import {Application} from "../../model/Application";
import {DetailedInfoRequest} from "../../dto/DetailedInfoRequest";
import {ResumeRequest} from "../../dto/ResumeRequest";
import {SignalResponse} from "../../model/SignalResponse";

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

  public listenOnPerformanceData(jobId: number): Observable<any> {
    return this.websocket.listenOnJobData(jobId);
  }

  public sendResumeSignal(appId: string): Observable<SignalResponse> {
    let msg: ResumeRequest = {
      "type": "ResumeRequest",
      "appId": appId,
      "correlationId": ""
    }

    return this.websocket.sendRequest<SignalResponse>(msg);
  }
}
