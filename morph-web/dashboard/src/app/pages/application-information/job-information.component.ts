import {AfterViewInit, Component, ElementRef, OnInit, ViewChild} from '@angular/core';
import {ApplicationService} from "../../shared/services/application.service";
import {Websocket} from "../../services/utils/websocket";


import {BasicApplication} from "../../model/BasicApplication";
import {JobInformationService} from "./job-information.service";
import {Application} from "../../model/Application";

@Component({
  selector: 'app-application-information',
  templateUrl: './job-information.component.html',
  styleUrls: ['./job-information.component.less']
})
export class JobInformationComponent implements OnInit, AfterViewInit {
  overallThroughput = (): string => `20.9 K tuple/sec`;
  processingLatency = (): string => `2230 ms`;

  @ViewChild('progressBarCol', { static: false }) progressBarCol!: ElementRef;
  progressBarWidth = 0; // the width of the progressbar

  basicApplication: BasicApplication;
  application: Application;

  constructor(private applicationService: ApplicationService, private websocket: Websocket, private applicationInformationService: JobInformationService) {
  }

  ngAfterViewInit() {
    this.setProgressBarWidth();
  }

  setProgressBarWidth() {
    this.progressBarWidth = this.progressBarCol.nativeElement.offsetWidth;
  }

  ngOnInit(): void {
    this.basicApplication = this.applicationService.getCurrentApplication();
    const applicationId: string = this.basicApplication.appId;

    this.websocket.connect("ws://localhost:5001/websocket");

    this.applicationInformationService.getHistoricalJob(applicationId).subscribe(res => {
      console.log(res);
      this.application = res;
    });
  }
}
