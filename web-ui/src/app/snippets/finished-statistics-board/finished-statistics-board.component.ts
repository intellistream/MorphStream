import {AfterViewInit, Component} from '@angular/core';
import * as Highcharts from 'highcharts';
import { Options } from 'highcharts';

@Component({
  selector: 'app-finished-statistics-board',
  templateUrl: './finished-statistics-board.component.html',
  styleUrls: ['./finished-statistics-board.component.less']
})
export class FinishedStatisticsBoardComponent implements AfterViewInit {
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
        ['exploration', 78490],
        ['tpg construction', 19573],
        ['other', 12322]
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
    },
    yAxis: {
      title: {
        text: 'Value'
      }
    },
    series: [
      {
        type: 'area',
        name: 'Throughput (10k tuples/s)',
        data: [2.1, 2.5, 1.9, 1.3, 2.1, 2.1, 2.3, 3.2, 3.0, 2.7]
      },
      {
        type: 'area',
        name: 'latency (s)',
        data: [1.9, 1.8, 1.9, 2.2, 2.3, 2.0, 2.1, 1.8, 1.9, 1.7]
      }]
  };

  ngAfterViewInit() {
    Highcharts.chart('throughputLatencyContainer', this.throughputLatencyData)
    Highcharts.chart('timeChartContainer', this.timeChartData);
  }
}
