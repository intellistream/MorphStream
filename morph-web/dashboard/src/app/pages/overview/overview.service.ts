import { Injectable } from '@angular/core';
import {Job} from "../../dto/Job";
import {HttpClient} from "@angular/common/http";
import {Observable} from "rxjs";

@Injectable({
  providedIn: 'root'
})
export class OverviewService {
  constructor(private http: HttpClient) {}

  /**
   * Get all jobs from backend
   */
  public getAllJobs(): Observable<Job[]> {
    return this.http.get<Job[]>("http://localhost:8080/jobInfo/get/all")
  }
}
