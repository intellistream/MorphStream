import {Component, ElementRef, ViewChild} from '@angular/core';
import {Application} from "../../model/application";

@Component({
  selector: 'app-home',
  templateUrl: './overview.component.html',
  styleUrls: ['./overview.component.less']
})
export class OverviewComponent {
  @ViewChild('runningAppContainer')
  scrollContainer!: ElementRef<HTMLElement>;

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
      id: 1,
      name: "Online Social Media Event Detection",
      numOfThreads: 8,
      cpu: "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz",
      ram: "32GB",
      startTime: "2023-Mar-10 09:15:27",
      duration: "00:08:17",
      isRunning: true,
      operators: []
    }
  ];

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
}
