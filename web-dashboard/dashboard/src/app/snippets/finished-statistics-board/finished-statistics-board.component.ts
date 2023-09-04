import {AfterViewInit, Component, Input} from '@angular/core';
import * as Highcharts from 'highcharts';
import { Options } from 'highcharts';
import {Application} from "../../model/Application";

@Component({
  selector: 'app-finished-statistics-board',
  templateUrl: './finished-statistics-board.component.html',
  styleUrls: ['./finished-statistics-board.component.less']
})
export class FinishedStatisticsBoardComponent implements AfterViewInit {
  @Input()
  job: Application;

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
    },
    series: [{
      type: 'pie',
      name: 'Processing Time',
      data: [
        {
          name: 'exploration',
          y: 78490,
          color: '#8BDB4D'
        },
        {
          name: 'tpg construction',
          y: 19573,
          color: '#0FB2E5'
        },
        {
          name: 'other',
          y: 12322,
          color: '#EF5A5A'
        }
      ]
    }]
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
    series: [
      {
        type: 'area',
        name: 'Throughput (10k tuples/s)',
        data: [2.1, 2.5, 1.9, 1.3, 2.1, 2.1, 2.3, 3.2, 3.0, 2.7],
        color: '#8BDB4D'
      },
      {
        type: 'area',
        name: 'latency (s)',
        data: [1.9, 1.8, 1.9, 2.2, 2.3, 2.0, 2.1, 1.8, 1.9, 1.7],
        color: '#0FB2E5'
      }]
  };

  ngAfterViewInit() {
    Highcharts.chart('throughputLatencyContainer', this.throughputLatencyData)
    Highcharts.chart('timeChartContainer', this.timeChartData);
  }
}
