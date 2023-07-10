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
      isRunning: true
    },
    {
      id: 1,
      name: "Online Social Media Event Detection",
      numOfThreads: 8,
      cpu: "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz",
      ram: "32GB",
      startTime: "2023-Mar-10 09:15:27",
      duration: "00:08:17",
      isRunning: true
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
      isRunning: false
    },
    {
      id: 3,
      name: "Grep Sum",
      numOfThreads: 2,
      cpu: "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz",
      ram: "32GB",
      startTime: "2023-Mar-05 02:16:11",
      duration: "00:09:46",
      isRunning: false
    },
    {
      id: 4,
      name: "Stream Ledger",
      numOfThreads: 2,
      cpu: "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz",
      ram: "32GB",
      startTime: "2023-Mar-08 15:05:33",
      duration: "00:12:01",
      isRunning: false
    },
    {
      id: 5,
      name: "Index-Based Window Join",
      numOfThreads: 4,
      cpu: "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz",
      ram: "32GB",
      startTime: "2023-Mar-09 20:55:34",
      duration: "00:36:17",
      isRunning: false
    }
  ];
}
