import {AfterViewInit, Component, ElementRef, OnInit, ViewChild} from '@angular/core';
import {ApplicationService} from "../../shared/services/application.service";
import {Websocket} from "../../services/utils/websocket";

import {BasicApplication} from "../../model/BasicApplication";
import {JobInformationService} from "./job-information.service";
import {Application} from "../../model/Application";
import {ActivatedRoute} from "@angular/router";

import * as d3 from 'd3';

@Component({
  selector: 'app-application-information',
  templateUrl: './job-information.component.html',
  styleUrls: ['./job-information.component.less']
})
export class JobInformationComponent implements OnInit, AfterViewInit {
  basicApplication: BasicApplication;
  application: Application;

  throughputLatencyData: any[] = [];
  timePieData: any[] = [];
  batchOptions: any[] = [];

  constructor(private route: ActivatedRoute,
              private applicationService: ApplicationService,
              private applicationInformationService: JobInformationService) {
  }

  drawGraph() {
    this.throughputLatencyData = [
      {
        name: 'Throughput (k tuples/s)',
        series: this.application.periodicalThroughput.map((value, index) => ({
          name: index.toString() + " s",
          value: value
        })),
      },
      {
        name: 'Latency (s)',
        series: this.application.periodicalLatency.map((value, index) => ({
          name: index.toString() + " s",
          value: value
        })),
      }
    ];

    this.timePieData = [
      {
        name: 'exploration time (ms)',
        value: this.application.schedulerTimeBreakdown.exploreTime,
      },
      {
        name: 'tpg construction time (ms)',
        value: this.application.schedulerTimeBreakdown.constructTime,
      },
      {
        name: 'other time (ms)',
        value: this.application.schedulerTimeBreakdown.abortTime +
          this.application.schedulerTimeBreakdown.trackingTime +
          this.application.schedulerTimeBreakdown.usefulTime,
      }
    ];

    this.batchOptions = [
      {value: '1', label: '1'},
      {value: '2', label: '2'},
      {value: '3', label: '3'},
      {value: '4', label: '4'},
      {value: '5', label: '5'}
    ];
  }

  private nodes = [{name: 'A'}, {name: 'B'}, {name: 'C'}, {name: 'D'}, {name: 'E'}, {name: 'F'}, {name: 'G'}, {name: 'H'}, {name: 'I'}, {name: 'J'}, {name: 'K'}, {name: 'L'}, {name: 'M'}, {name: 'N'}];
  private links = [{source: 'A', target: 'B', type: 'LD'}, {source: 'A', target: 'N', type: 'LD'}, {source: 'H', target: 'J', type: 'PD'}, {source: 'K', target: 'L', type: 'TD'},
    {source: 'B', target: 'C', type: 'PD'}, {source: 'C', target: 'D', type: 'LD'}, {source: 'C', target: 'K', type: 'TD'}, {source: 'H', target: 'M', type: 'LD'}, {source: 'M', target: 'N', type: 'PD'},
    {source: 'E', target: 'F', type: 'TD'}, {source: 'G', target: 'I', type: 'TD'}, {source: 'L', target: 'F', type: 'LD'}];

  private nodesSelection: any;
  private linksSelection: any;

  @ViewChild('graphContainer') private graphContainer!: ElementRef;
  private svg: any;
  private simulation: any;

  ngAfterViewInit() {
    this.createGraph();
  }

  createGraph() {
    this.svg = d3.select(this.graphContainer.nativeElement)
      .append('svg')
      .attr('width', 500)
      .attr('height', 400)
      .attr("viewBox", [0, 0, 640, 480])

    // let width = this.svg.node().clientWidth;
    // let heigth = this.svg.node().clientHeight;
    //
    // this.svg.attr("viewBox", [0, 0, 640, 480]);

    this.simulation = d3.forceSimulation(this.nodes)
      .force('charge', d3.forceManyBody().strength(-20))
      .force('link', d3.forceLink(this.links).id((d: any) => d.name))
      .force('center', d3.forceCenter(250, 200));

    this.linksSelection = this.svg.selectAll('.link')
      .data(this.links)
      .enter().append('line')
      .attr('class', 'link')
      .style("stroke", (d: any) => {
        if (d.type == "TD") {
          return "#94A0CE"
        } else if (d.type == "LD") {
          return "#B7C099"
        } else {
          return "#BE6F8A"
        }
      })
      .style('stroke-dasharray', (d: any) => {
        if (d.type === 'PD') {
          return '5,5';
        } else {
          return 'none';
        }
      })
      .style('stroke-width', 3);

    this.nodesSelection = this.svg.selectAll('.node')
      .data(this.nodes)
      .enter().append('circle')
      .attr('class', 'node')
      .attr('r', 4)
      .style("fill", "#e79722")

    this.simulation.on('tick', this.tick.bind(this));

    this.svg.call(d3.zoom()
      .extent([[0, 0], [648, 480]])
      .scaleExtent([0.5, 10])
      .on("zoom", this.zoomed.bind(this)));

    this.simulation.alpha(1).restart();
  }

  tick() {
    this.nodesSelection
      .attr('cx', (d: any) => d.x)
      .attr('cy', (d: any) => d.y);

    this.linksSelection
      .attr('x1', (d: any) => d.source.x)
      .attr('y1', (d: any) => d.source.y)
      .attr('x2', (d: any) => d.target.x)
      .attr('y2', (d: any) => d.target.y);
  }

  zoomed({transform}) {
    this.svg.selectAll('.node').attr('transform', transform);
    this.svg.selectAll('.link').attr('transform', transform);
  }

  ngOnInit(): void {
    this.route.params.subscribe(params => {
      const jobId = params['id'];

      this.applicationInformationService.getHistoricalJob(jobId).subscribe(res => {
        this.application = res;
        this.basicApplication = this.applicationService.getCurrentApplication();

        this.applicationInformationService.listenOnPerformanceData(jobId).subscribe(
          res => {
            console.log(res);
          }
        )
        this.drawGraph();
      });

    });
  }
}
