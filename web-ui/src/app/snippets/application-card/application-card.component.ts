import {Component, Input} from '@angular/core';
import {Application} from "../../model/application";
import {Router} from "@angular/router";

@Component({
  selector: 'app-application-card',
  templateUrl: './application-card.component.html',
  styleUrls: ['./application-card.component.less']
})

export class ApplicationCardComponent {
  @Input() application: Application = {
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
  }

  @Input() cardWidth: string = "480px";
  @Input()  cardHeight: string = "270px";

  constructor(private router: Router) {
  }

  navigateToAppDetails() {
    this.router.navigate(['overview/application-details']);
  }
}
