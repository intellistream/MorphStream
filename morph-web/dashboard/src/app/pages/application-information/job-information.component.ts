import { Component, ElementRef, OnInit, ViewChild} from '@angular/core';

import {JobInformationService} from "./job-information.service";
import {Job} from "../../model/Job";
import {ActivatedRoute} from "@angular/router";

import * as d3 from 'd3';
import {
  NzGraphComponent,
  NzGraphData,
  NzGraphDataDef,
  NzGraphZoomDirective,
} from "ng-zorro-antd/graph";

@Component({
  selector: 'app-application-information',
  templateUrl: './job-information.component.html',
  styleUrls: ['./job-information.component.less']
})
export class JobInformationComponent implements OnInit {
  @ViewChild('tpgContainer') private tpgContainer!: ElementRef; // tpg container
  @ViewChild(NzGraphZoomDirective, { static: true }) zoomController!: NzGraphZoomDirective;

  job: Job; // job entity

  tpgSvg: any;            // tpg svg
  tpgSvgSimulation: any;  // tpg graph drawing force simulation
  isTpgModalVisible = false;

  OperatorGraphData: NzGraphDataDef = {nodes: [], edges: []} // operator graph data {v: '1', w: '2'}, {id: '101', label: 'Spout'}
  nzOperatorGraphData: any; // operator graph data for nz-graph

  // Batch data
  tpgBatchOptions: any[] = [];
  throughputAndLatency: any[] = [];
  timePieData: any[] = [];
  latestBatches: {[key: string]: number} = {} // key: operator name, value: latest batch

  // batch-tpg data
  tpgNodes = [{name: 'A'}, {name: 'B'}, {name: 'C'}, {name: 'D'}, {name: 'E'}, {name: 'F'}, {name: 'G'}, {name: 'H'}, {name: 'I'}, {name: 'J'}, {name: 'K'}, {name: 'L'}, {name: 'M'}, {name: 'N'}];
  tpgLinks = [{source: 'A', target: 'B', type: 'LD'}, {source: 'A', target: 'N', type: 'LD'}, {source: 'H', target: 'J', type: 'PD'}, {source: 'K', target: 'L', type: 'TD'},
    {source: 'B', target: 'C', type: 'PD'}, {source: 'C', target: 'D', type: 'LD'}, {source: 'C', target: 'K', type: 'TD'}, {source: 'H', target: 'M', type: 'LD'}, {source: 'M', target: 'N', type: 'PD'},
    {source: 'E', target: 'F', type: 'TD'}, {source: 'G', target: 'I', type: 'TD'}, {source: 'L', target: 'F', type: 'LD'}];

  nodesSelections: any;
  linksSelections: any;

  constructor(private route: ActivatedRoute, private jobInformationService: JobInformationService) {}

  ngOnInit(): void {
    this.route.params.subscribe(params => {
      const jobId = params['id'];
      this.jobInformationService.getJob(jobId).subscribe(res => {
        this.job = res;

        // add operators to operator graph
        for (let i = 0; i < this.job.operators.length; i++) {
          this.OperatorGraphData.nodes.push({id: this.job.operators[i].id, label: this.job.operators[i].name});
          if (i > 0) {
            this.OperatorGraphData.edges.push({v: this.job.operators[i-1].id, w: this.job.operators[i].id});
          }
        }

        this.drawOperatorGraph();
        this.drawTpgGraph();
      });
    });
  }

  /**
   * Draw the statistic graph
   */
  drawStatisticGraph() {
    this.throughputAndLatency = [
      {
        name: 'Throughput (k tuples/s)',
        series: this.job.periodicalThroughput.map((value, index) => ({
          name: index.toString() + " s",
          value: value
        })),
      },
      {
        name: 'Latency (s)',
        series: this.job.periodicalLatency.map((value, index) => ({
          name: index.toString() + " s",
          value: value
        })),
      }
    ];

    this.timePieData = [
      {
        name: 'exploration time (ms)',
        value: this.job.schedulerTimeBreakdown.exploreTime,
      },
      {
        name: 'tpg construction time (ms)',
        value: this.job.schedulerTimeBreakdown.constructTime,
      },
      {
        name: 'other time (ms)',
        value: this.job.schedulerTimeBreakdown.abortTime +
          this.job.schedulerTimeBreakdown.trackingTime +
          this.job.schedulerTimeBreakdown.usefulTime,
      }
    ];

    this.tpgBatchOptions = [
      {value: '1', label: '1'},
      {value: '2', label: '2'},
      {value: '3', label: '3'},
      {value: '4', label: '4'},
      {value: '5', label: '5'}
    ];
  }

  /**
   * Draw the operator graph
   */
  drawOperatorGraph() {
    this.nzOperatorGraphData = new NzGraphData(this.OperatorGraphData);
  }

  /**
   * Callback when the operator graph is initialized
   */
  onOperatorGraphInitialized(_ele: NzGraphComponent): void {
    this.zoomController?.fitCenter();
  }

  /**
   * Draw the tpg graph
   */
  drawTpgGraph() {
    this.tpgSvg = d3.select(this.tpgContainer.nativeElement)
      .append('svg')
      .attr('width', '100%')
      .attr('height', '100%')
      .attr("viewBox", [0, 0, 640, 480]);

    // @ts-ignore
    this.tpgSvgSimulation = d3.forceSimulation(this.tpgNodes)
      .force('charge', d3.forceManyBody().strength(-20))
      .force('link', d3.forceLink(this.tpgLinks).id((d: any) => d.name))
      .force('center', d3.forceCenter(250, 200));

    this.linksSelections = this.tpgSvg.selectAll('.link')
      .data(this.tpgLinks)
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

    this.nodesSelections = this.tpgSvg.selectAll('.node')
      .data(this.tpgNodes)
      .enter().append('circle')
      .attr('class', 'node')
      .attr('r', 4)
      .style("fill", "#e79722");

    this.tpgSvgSimulation.on('tick', this.simulationTick.bind(this));
    this.tpgSvg.call(d3.zoom()
      .extent([[0, 0], [648, 480]])
      .scaleExtent([0.5, 10])
      .on("zoom", this.tpgZoomed.bind(this)));
    this.tpgSvgSimulation.alpha(1).restart();
  }

  simulationTick() {
    this.nodesSelections
      .attr('cx', (d: any) => d.x)
      .attr('cy', (d: any) => d.y);
    this.linksSelections
      .attr('x1', (d: any) => d.source.x)
      .attr('y1', (d: any) => d.source.y)
      .attr('x2', (d: any) => d.target.x)
      .attr('y2', (d: any) => d.target.y);
  }

  onResume() {
    // this.jobInformationService.sendResumeSignal(this.job.jobId).subscribe(res => {
    //   if (res.jobStart) {
    //     for (let i = 0; i < this.job.operators.length; i++) {
    //       setInterval(() => this.jobInformationService.sendPerformanceRequest(this.job.jobId, this.job.operators[i].name, this.job.operators[i].lastBatch), 1000);  // query every 1 second
    //     }
    //   }
    // });
  }

  /**
   * Callback when the tpg graph is zoomed
   * @param transform
   */
  tpgZoomed({transform}) {
    this.tpgSvg.selectAll('.node').attr('transform', transform);
    this.tpgSvg.selectAll('.link').attr('transform', transform);
  }

  // TODO: tpg modal is not in use for now
  onExpandTpgModal() {
    this.isTpgModalVisible = true;
  }

  onClearTpgModal() {
    this.isTpgModalVisible = false;
  }
}
