import { Component } from '@angular/core';
import {Application} from "../../../model/application";
import {Router} from "@angular/router";
import {ApplicationService} from "../../../shared/services/application.service";

@Component({
  selector: 'app-pocessing-applications',
  templateUrl: './processing-applications.component.html',
  styleUrls: ['./processing-applications.component.less']
})
export class ProcessingApplicationsComponent {
  constructor(private router: Router, private applicationService: ApplicationService) {
  }

  // Data for testing
  runningApplications: Application[] = [
    {
      id: 1,
      name: "Online Social Media Event Detection",
      numOfThreads: 8,
      cpu: "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz",
      ram: "32GB",
      startTime: "2023-Mar-10 09:15:27",
      duration: "00:08:17",
      isRunning: true,
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
          numOfInstances: 4,
          throughput: 27.8, // tuples/s
          latency: 345.4,  // ms
          explorationStrategy: "Structured Exploration",
          schedulingGranularity: "Fine-Grained Unit",
          abortHandling: "Eager Abort",
          numOfTD: 120,
          numOfLD: 560,
          numOfPD: 80
        },
        {
          id: 3,
          name: "Word Updater",
          numOfInstances: 4,
          throughput: 21.3, // tuples/s
          latency: 438.8,  // ms
          explorationStrategy: "Non-Structured Exploration",
          schedulingGranularity: "Fine-Grained Unit",
          abortHandling: "Eager Abort",
          numOfTD: 588,
          numOfLD: 790,
          numOfPD: 422
        },
        {
          id: 4,
          name: "Trend Calculator",
          numOfInstances: 4,
          throughput: 24.9, // tuples/s
          latency: 266.3,  // ms
          explorationStrategy: "Structured Exploration",
          schedulingGranularity: "Fine-Grained Unit",
          abortHandling: "Lazy Abort",
          numOfTD: 632,
          numOfLD: 450,
          numOfPD: 120
        }
      ]
    },
    {
      id: 2,
      name: "Online Social Media Event Detection",
      numOfThreads: 8,
      cpu: "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz",
      ram: "32GB",
      startTime: "2023-Mar-10 09:15:27",
      duration: "00:08:17",
      isRunning: true,
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
          numOfTD: 1234,
          numOfLD: 4321,
          numOfPD: 134
        },
        {
          id: 3,
          name: "Word Updater",
          numOfInstances: 8,
          throughput: 12.2, // tuples/s
          latency: 314.1,  // ms
          explorationStrategy: "Non-Structured Exploration",
          schedulingGranularity: "Fine-Grained Unit",
          abortHandling: "Lazy Abort",
          numOfTD: 346,
          numOfLD: 568,
          numOfPD: 123
        },
      ]
    }
  ];

  navigateToAppDetails(application: Application) {
    this.applicationService.setCurrentApplication(application);
    this.router.navigate(['overview/application-details']);
  }
}
