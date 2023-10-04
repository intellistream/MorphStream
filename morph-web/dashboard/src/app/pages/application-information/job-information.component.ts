import {AfterViewInit, Component, ElementRef, OnInit, ViewChild} from '@angular/core';
import {ApplicationService} from "../../shared/services/application.service";
import {Websocket} from "../../services/utils/websocket";

import {BasicApplication} from "../../model/BasicApplication";
import {JobInformationService} from "./job-information.service";
import {Application} from "../../model/Application";
import {ActivatedRoute} from "@angular/router";

import * as d3 from 'd3';
import {NzCascaderOption} from "ng-zorro-antd/cascader";

@Component({
  selector: 'app-application-information',
  templateUrl: './job-information.component.html',
  styleUrls: ['./job-information.component.less']
})
export class JobInformationComponent implements OnInit {
  basicApplication: BasicApplication;
  application: Application;

  throughputLatencyData: any[] = [];
  timePieData: any[] = [];

  batchOptions: any[] = [];


  constructor(private route: ActivatedRoute,
              private applicationService: ApplicationService,
              private websocket: Websocket,
              private applicationInformationService: JobInformationService,
              private el: ElementRef) {
  }

  ngOnInit(): void {
    this.websocket.connect("ws://localhost:5001/websocket");

    this.route.params.subscribe(params => {
      const jobId = params['id'];

      this.applicationInformationService.getHistoricalJob(jobId).subscribe(res => {
        this.application = res;
        this.basicApplication = this.applicationService.getCurrentApplication();

        this.throughputLatencyData = [
          {
            name: 'Throughput (k tuples/s)',
            series: this.application.periodicalThroughput.map((value, index) => ({
              name: index.toString() + " s",
              value: value
            })),
            // color: '#8BDB4D'
          },
          {
            name: 'Latency (s)',
            series: this.application.periodicalLatency.map((value, index) => ({
              name: index.toString() + " s",
              value: value
            })),
            // color: '#0FB2E5'
          }
        ];

        this.timePieData = [
          {
            name: 'exploration time (ms)',
            value: this.application.schedulerTimeBreakdown.exploreTime,
            // color: '#8BDB4D'
          },
          {
            name: 'tpg construction time (ms)',
            value: this.application.schedulerTimeBreakdown.constructTime,
            // color: '#0FB2E5'
          },
          {
            name: 'other time (ms)',
            value: this.application.schedulerTimeBreakdown.abortTime +
              this.application.schedulerTimeBreakdown.trackingTime +
              this.application.schedulerTimeBreakdown.usefulTime,
            // color: '#EF5A5A'
          }
        ];

        this.batchOptions = [
          {
            value: '1',
            label: '1',
          },
          {
            value: '2',
            label: '2',
          },
          {
            value: '3',
            label: '3',
          },
          {
            value: '4',
            label: '4',
          },
          {
            value: '5',
            label: '5',
          }
        ];
      });
    });
  }
}
