import { Injectable } from '@angular/core';
import {Websocket} from "../../services/utils/websocket";
import {Observable} from "rxjs";
import {Job} from "../../model/Job";
import {DetailedInfoRequest} from "../../dto/DetailedInfoRequest";
import {ResumeRequest} from "../../dto/ResumeRequest";
import {SignalResponse} from "../../model/SignalResponse";
import {Performance} from "../../model/Performance";
import {PerformanceRequest} from "../../dto/PerformanceRequest";
import {HttpClient} from "@angular/common/http";

@Injectable({
  providedIn: 'root'
})
export class JobInformationService {
  constructor(private http: HttpClient) {}

  /**
   * Get job information by job id
   * @param jobId
   */
  public getJob(jobId: string): Observable<Job> {
    return this.http.get<Job>(`http://localhost:8080/jobInfo/get/${jobId}`);
  }

  // public getHistoricalJob(appId: string): Observable<Job> {
  //   let msg: DetailedInfoRequest = {
  //     "type": "DetailInfoRequest",
  //     "appId": appId,
  //     "correlationId": ""
  //   }
  //
  //   return this.websocket.sendRequest<Job>(msg);
  // }

  // public listenOnPerformanceData(jobId: string): Observable<Performance> {
  //   return this.websocket.listenOnPerformanceData(jobId);
  // }

  // public sendPerformanceRequest(jobId: string, operator: string, latestBatch: number) {
  //   let msg: PerformanceRequest = {
  //     "type": "Performance",
  //     "appId": jobId,
  //     "correlationId": "",
  //     "operator": operator, // sl
  //     "latestBatch": latestBatch
  //   }
  //   this.websocket.sendPerformanceRequest(msg)
  //   return
  // }

  // public sendResumeSignal(appId: string): Observable<SignalResponse> {
  //   let msg: ResumeRequest = {
  //     "type": "Signal",
  //     "appId": appId,
  //     "correlationId": "",
  //     "signal": "start"
  //   }
  //
  //   return this.websocket.sendRequest<SignalResponse>(msg);
  // }
}
