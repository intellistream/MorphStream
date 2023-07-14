import { Component } from '@angular/core';
import {Application} from "../../../model/application";
import {Router} from "@angular/router";

@Component({
  selector: 'app-finished-applications',
  templateUrl: './finished-applications.component.html',
  styleUrls: ['./finished-applications.component.less']
})
export class FinishedApplicationsComponent {
  constructor(private router: Router) {
  }

  completedApplications: Application[] = [
    {
      id: 2,
      name: "Index-Based Window Join",
      numOfThreads: 4,
      cpu: "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz",
      ram: "32GB",
      startTime: "2023-Mar-09 20:55:34",
      duration: "00:36:17",
      isRunning: false,
      operators: []
    },
    {
      id: 3,
      name: "Grep Sum",
      numOfThreads: 2,
      cpu: "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz",
      ram: "32GB",
      startTime: "2023-Mar-05 02:16:11",
      duration: "00:09:46",
      isRunning: false,
      operators: []
    },
    {
      id: 4,
      name: "Stream Ledger",
      numOfThreads: 2,
      cpu: "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz",
      ram: "32GB",
      startTime: "2023-Mar-08 15:05:33",
      duration: "00:12:01",
      isRunning: false,
      operators: []
    },
    {
      id: 5,
      name: "Index-Based Window Join",
      numOfThreads: 4,
      cpu: "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz",
      ram: "32GB",
      startTime: "2023-Mar-09 20:55:34",
      duration: "00:36:17",
      isRunning: false,
      operators: []
    }
  ];

  navigateToAppDetails() {
    this.router.navigate(['overview/application-details']);
  }
}
