import {AfterViewInit, Component, ElementRef, OnInit, ViewChild} from '@angular/core';
import {ApplicationService} from "../../shared/services/application.service";

import {BasicApplication} from "../../model/BasicApplication";
import {JobInformationService} from "./job-information.service";
import {Application} from "../../model/Application";
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
export class JobInformationComponent implements OnInit, AfterViewInit {
  basicApplication: BasicApplication;
  application: Application;

  throughputLatencyData: any[] = [];
  timePieData: any[] = [];
  batchOptions: any[] = [];

  @ViewChild('tpgContainer') private tpgContainer!: ElementRef;
  @ViewChild(NzGraphZoomDirective, { static: true }) zoomController!: NzGraphZoomDirective;

  private tpgSvg: any;
  private simulation: any;


  isTpgModalVisible = false;
  tpgModalTitle = "TPG";

  constructor(private route: ActivatedRoute,
              private applicationService: ApplicationService,
              private applicationInformationService: JobInformationService) {
  }

  drawStatisticGraph() {
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

  ngAfterViewInit() {
    this.drawTpg();
  }

  graphData: NzGraphDataDef = {
    nodes: [
      {id: '1', label: 'Spout'},
      {id: '2', label: 'Tweet Registrant'},
      {id: '3', label: 'Word Updater'},
      {id: '4', label: 'Sink'},
    ],
    edges: [
      {v: '1', w: '2'},
      {v: '2', w: '3'},
      {v: '3', w: '4'},
    ]
  };
  nzGraphData: any;
  // rankDirection: NzRankDirection = 'LR';
  drawOperators() {
    this.nzGraphData = new NzGraphData(this.graphData);
  }

  graphInitialized(_ele: NzGraphComponent): void {
    // Only nz-graph-zoom enabled, you should run `fitCenter` manually
    this.zoomController?.fitCenter();
  }

  drawTpg() {
    this.tpgSvg = d3.select(this.tpgContainer.nativeElement)
      .append('svg')
      // .attr('width', 500)
      .attr('width', '100%')
      .attr('height', '100%')
      .attr("viewBox", [0, 0, 640, 480]);

    // @ts-ignore
    this.simulation = d3.forceSimulation(this.nodes)
      .force('charge', d3.forceManyBody().strength(-20))
      .force('link', d3.forceLink(this.links).id((d: any) => d.name))
      .force('center', d3.forceCenter(250, 200));

    this.linksSelection = this.tpgSvg.selectAll('.link')
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

    this.nodesSelection = this.tpgSvg.selectAll('.node')
      .data(this.nodes)
      .enter().append('circle')
      .attr('class', 'node')
      .attr('r', 4)
      .style("fill", "#e79722")

    this.simulation.on('tick', this.tick.bind(this));

    this.tpgSvg.call(d3.zoom()
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
    this.tpgSvg.selectAll('.node').attr('transform', transform);
    this.tpgSvg.selectAll('.link').attr('transform', transform);
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
        this.drawStatisticGraph();
        this.drawOperators();
      });
    });
  }

  onExpandTpg() {
    this.isTpgModalVisible = true;
  }
  onTpgModalCancel() {
    this.isTpgModalVisible = false;
  }
}
