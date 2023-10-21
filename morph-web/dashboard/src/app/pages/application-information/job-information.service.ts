import { Injectable } from '@angular/core';
import {Observable} from "rxjs";
import {Job} from "../../model/Job";
import {HttpClient} from "@angular/common/http";
import {Batch} from "../../model/Batch";

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

  public startJob(jobId: string): Observable<boolean> {
    return this.http.post<boolean>(`http://localhost:8080/api/signal/start/${jobId}`, null);
  }

  public stopJob(jobId: string): Observable<boolean> {
    return this.http.post<boolean>(`http://localhost:8080/api/signal/stop/${jobId}`, null);
  }

  public getBatchById(jobId: string, operatorId: string, batchId: string): Observable<Batch> {
    return this.http.get<Batch>(`http://localhost:8080/batchInfo/get/${jobId}/${batchId}/${operatorId}`);
  }
}
