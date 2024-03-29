import {AfterViewInit, Component, ElementRef, Input, OnInit} from '@angular/core';
import {Job} from "../../model/Job";

@Component({
  selector: 'app-finished-statistics-board',
  templateUrl: './finished-statistics-board.component.html',
  styleUrls: ['./finished-statistics-board.component.less']
})
export class FinishedStatisticsBoardComponent implements OnInit {
  @Input()
  job!: Job;

  constructor(private el: ElementRef) {}


  throughputLatencyData: any[] = [];

  ngOnInit(): void {

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
