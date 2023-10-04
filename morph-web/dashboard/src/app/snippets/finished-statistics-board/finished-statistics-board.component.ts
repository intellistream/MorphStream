import {AfterViewInit, Component, ElementRef, Input, OnInit} from '@angular/core';
import * as Highcharts from 'highcharts';
import { Options } from 'highcharts';
import {Application} from "../../model/Application";

import * as d3 from 'd3';

@Component({
  selector: 'app-finished-statistics-board',
  templateUrl: './finished-statistics-board.component.html',
  styleUrls: ['./finished-statistics-board.component.less']
})
export class FinishedStatisticsBoardComponent implements AfterViewInit, OnInit {
  @Input()
  job!: Application;

  constructor(private el: ElementRef) {}

  timeChartData: Options = {
    chart: {
      type: 'pie',
      backgroundColor: 'rgba(67,78,131, 10)'
    },
    title: {
      text: 'Processing Time Distribution (ms)',
      style: {
        color: '#ffffff',
        fontFamily: 'Inter'
      }
    }
  };

  throughputLatencyData: any[] = [];

  ngAfterViewInit() {
    console.log(this.job)
    // Highcharts.chart('throughputLatencyContainer', this.throughputLatencyData)
    // Highcharts.chart('timeChartContainer', this.timeChartData);
  }

  ngOnInit(): void {
    this.timeChartData.series = [{
      type: 'pie',
      name: 'Processing Time',
      data: [
        {
          name: 'exploration',
          y: this.job.schedulerTimeBreakdown.exploreTime,
          color: '#8BDB4D'
        },
        {
          name: 'tpg construction',
          y: this.job.schedulerTimeBreakdown.constructTime,
          color: '#0FB2E5'
        },
        {
          name: 'other',
          y: this.job.schedulerTimeBreakdown.abortTime + this.job.schedulerTimeBreakdown.trackingTime + this.job.schedulerTimeBreakdown.usefulTime,
          color: '#EF5A5A'
        }
      ]
    }]

    this.throughputLatencyData = [
      {
        name: 'Throughput (k tuples/s)',
        series: this.job.periodicalThroughput.map((value, index) => ({
          name: index.toString() + " s",
          value: value
        })),
        color: '#8BDB4D'
      },
      {
        name: 'Latency (s)',
        series: this.job.periodicalLatency.map((value, index) => ({
          name: index.toString() + " s",
          value: value
        })),
        color: '#0FB2E5'
      }
    ];
  }
}
