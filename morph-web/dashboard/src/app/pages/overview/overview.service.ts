import { Injectable } from '@angular/core';
import {Job} from "../../model/Job";
import {HttpClient} from "@angular/common/http";

@Injectable({
  providedIn: 'root'
})
export class OverviewService {
  constructor(private http: HttpClient) {}

  /**
   * Get all jobs from backend
   */
  public getAllJobs() {
    return this.http.get<Job[]>("http://localhost:8080/jobInfo/get/all")
  }
}
