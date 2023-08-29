import { Component } from '@angular/core';
import {Application} from "../../../model/application";
import {Router} from "@angular/router";
import {ApplicationService} from "../../../shared/services/application.service";

@Component({
  selector: 'app-finished-applications',
  templateUrl: './finished-applications.component.html',
  styleUrls: ['./finished-applications.component.less']
})
export class FinishedApplicationsComponent {
  constructor(private router: Router, private applicationService: ApplicationService) {
  }

  completedApplications: Application[] = [
    {
      id: 3,
      name: "Index-Based Window Join",
      numOfThreads: 4,
      cpu: "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz",
      ram: "32GB",
      startTime: "2023-Mar-09 20:55:34",
      duration: "00:36:17",
      isRunning: false,
      operators: [
        {
          id: 1,
          name: "Spout",
          numOfInstances: 1,
          throughput: -1, // tuples/s
          latency: -1,  // ms
          explorationStrategy: "NA",
          schedulingGranularity: "NA",
          abortHandling: "NA",
          numOfTD: -1,
          numOfLD: -1,
          numOfPD: -1
        },
        {
          id: 2,
          name: "Tweet Registrant",
          numOfInstances: 8,
          throughput: 10, // tuples/s
          latency: 200.1,  // ms
          explorationStrategy: "Non-Structured Exploration",
          schedulingGranularity: "Fine-Grained Unit",
          abortHandling: "Lazy Abort",
          numOfTD: 754,
          numOfLD: 111,
          numOfPD: 564
        }
      ]
    },
    {
      id: 4,
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
      id: 5,
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
      id: 6,
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

  navigateToAppDetails(application: Application) {
    this.applicationService.setCurrentApplication(application);
    this.router.navigate(['overview/application-details']);
  }
}
