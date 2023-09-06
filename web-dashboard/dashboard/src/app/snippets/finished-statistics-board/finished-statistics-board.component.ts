import {AfterViewInit, Component, Input, OnInit} from '@angular/core';
import * as Highcharts from 'highcharts';
import { Options } from 'highcharts';
import {Application} from "../../model/Application";

@Component({
  selector: 'app-finished-statistics-board',
  templateUrl: './finished-statistics-board.component.html',
  styleUrls: ['./finished-statistics-board.component.less']
})
export class FinishedStatisticsBoardComponent implements AfterViewInit, OnInit {
  @Input()
  job!: Application;

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

  throughputLatencyData: Options = {
    chart: {
      type: 'area',
      backgroundColor: 'rgba(67,78,131, 10)'
    },
    title: {
      text: 'Throughput & Latency',
      style: {
        color: '#ffffff',
        fontFamily: 'Inter'
      }
    },
    xAxis: {
      categories: ['100', '200', '300', '400', '500', '600', '700', '800', '900', '1000'],
      lineColor: '#ffffff',
      labels: {
        style: {
          color: '#ffffff'
        }
      }
    },
    yAxis: {
      title: {
        text: 'Value',
        style: {
          color: '#ffffff',
        }
      },
      labels: {
        style: {
          color: '#ffffff'
        }
      },
    },
    legend: {
      itemStyle: {
        color: '#ffffff' // 图例项的颜色
      }
    },
  };

  ngAfterViewInit() {
    console.log(this.job)
    Highcharts.chart('throughputLatencyContainer', this.throughputLatencyData)
    Highcharts.chart('timeChartContainer', this.timeChartData);
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

    this.throughputLatencyData.series = [
      {
        type: 'area',
        name: 'Throughput (10k tuples/s)',
        data: this.job.periodicalThroughput,
        color: '#8BDB4D'
      },
      {
        type: 'area',
        name: 'latency (s)',
        data: this.job.periodicalLatency,
        color: '#0FB2E5'
      }]
  }
}
