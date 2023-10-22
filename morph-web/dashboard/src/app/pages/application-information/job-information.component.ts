import {Component, ElementRef, OnInit, ViewChild} from '@angular/core';

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
import {Batch} from "../../model/Batch";
import {FormControl, FormGroup, NonNullableFormBuilder, Validators} from "@angular/forms";
import {NzMessageService} from "ng-zorro-antd/message";

@Component({
  selector: 'app-application-information',
  templateUrl: './job-information.component.html',
  styleUrls: ['./job-information.component.less']
})
export class JobInformationComponent implements OnInit {
  @ViewChild('tpgContainer') private tpgContainer!: ElementRef; // tpg container
  @ViewChild(NzGraphZoomDirective, {static: true}) zoomController!: NzGraphZoomDirective;

  job: Job; // job entity
  statisticBatch: Batch; // statistic batch entity
  tpgBatch: Batch | null = null; // TPG batch entity

  tpgSvg: any;            // tpg svg
  tpgSvgSimulation: any;  // tpg graph drawing force simulation
  isTpgModalVisible = false;

  OperatorGraphData: NzGraphDataDef = {nodes: [], edges: []} // operator graph data {v: '1', w: '2'}, {id: '101', label: 'Spout'}
  nzOperatorGraphData: any; // operator graph data for nz-graph

  // Batch data
  batchOptions: any[] = [];
  throughputAndLatency: any[] = [
    {name: 'Throughput (k tuples/s)', series: []},
    {name: 'Latency (s)', series: []}];

  timePieData: any[] = [{name: 'overhead time time (ms)', value: 0,},
    {name: 'stream time time (ms)', value: 0,},
    {name: 'overhead time (ms)', value: 0,}];

  tpgData: any[] = [{name: 'TD', value: 0,}, {name: 'LD', value: 0,}, {name: 'PD', value: 0,}];

  operatorLatestBatchNum: { [key: string]: number } = {} // key: operator name, value: latest batch

  // batch-tpg data
  tpgNodes = [{name: 'A'}, {name: 'B'}, {name: 'C'}, {name: 'D'}, {name: 'E'}, {name: 'F'}, {name: 'G'}, {name: 'H'}, {name: 'I'}, {name: 'J'}, {name: 'K'}, {name: 'L'}, {name: 'M'}, {name: 'N'}];
  tpgLinks = [{source: 'A', target: 'B', type: 'LD'}, {source: 'A', target: 'N', type: 'LD'}, {
    source: 'H',
    target: 'J',
    type: 'PD'
  }, {source: 'K', target: 'L', type: 'TD'},
    {source: 'B', target: 'C', type: 'PD'}, {source: 'C', target: 'D', type: 'LD'}, {
      source: 'C',
      target: 'K',
      type: 'TD'
    }, {source: 'H', target: 'M', type: 'LD'}, {source: 'M', target: 'N', type: 'PD'},
    {source: 'E', target: 'F', type: 'TD'}, {source: 'G', target: 'I', type: 'TD'}, {
      source: 'L',
      target: 'F',
      type: 'LD'
    }];
  numOfTD = 0;
  numOfLD = 0;
  numOfPD = 0;

  nodesSelections: any;
  linksSelections: any;

  jobStarted = false;

  batchForm: FormGroup<{
    batch: FormControl<string>;
    operator: FormControl<string>;
  }>;

  tpgForm: FormGroup<{
    batch: FormControl<string>;
    operator: FormControl<string>;
  }>;

  constructor(private route: ActivatedRoute,
              private jobInformationService: JobInformationService,
              private fb: NonNullableFormBuilder,
              private message: NzMessageService) {
    this.batchForm = this.fb.group({
      batch: ['', [Validators.required]],
      operator: ['', [Validators.required]]
    });
    this.tpgForm = this.fb.group({
      batch: ['', [Validators.required]],
      operator: ['', [Validators.required]]
    });
  }

  ngOnInit(): void {
    this.route.params.subscribe(params => {
      const jobId = params['id'];
      this.jobInformationService.getJob(jobId).subscribe(res => {
        this.job = res;
        this.jobStarted = this.job.isRunning;

        // add operators to operator graph
        for (let i = 0; i < this.job.operators.length; i++) {
          // this.operatorLatestBatchNum[this.job.operators[i].id] = 0;  // initialize the latest batch number
          this.operatorLatestBatchNum["sl"] = 1;  // initialize the latest batch number
          this.OperatorGraphData.nodes.push({id: this.job.operators[i].id, label: this.job.operators[i].name});
          if (i > 0) {
            this.OperatorGraphData.edges.push({v: this.job.operators[i - 1].id, w: this.job.operators[i].id});
          }
        }
        this.initializeData();

        this.drawOperatorGraph();
        // this.drawTpgGraph();
        setInterval(() => {
          this.update();
        }, 1000);
      });
    });
  }

  /**
   * Initialize the throughput and latency data
   */
  initializeData() {
    for (let i = 0; i < 10; i++) {
      this.throughputAndLatency[0].series.push({name: `batch${i}`, value: 0});
      this.throughputAndLatency[1].series.push({name: `batch${i}`, value: 0});
    }
  }

  /**
   * Update the runtime data
   */
  update() {
    this.jobInformationService.getBatchById(this.job.jobId, 'sl', this.operatorLatestBatchNum['sl'].toString()).subscribe(res => {
      if (res) {
        this.batchOptions.push({
          value: this.operatorLatestBatchNum['sl'].toString(),
          label: this.operatorLatestBatchNum['sl'].toString()
        });
        this.updatePerformanceGraph(res);
        this.operatorLatestBatchNum['sl']++;  // update the latest batch number
      }
    });
  }

  /**
   * Update the performance graph
   */
  updatePerformanceGraph(batch: Batch) {
    if (batch.batchId < 10) {
      this.throughputAndLatency[0].series[batch.batchId].value = batch.throughput;
      this.throughputAndLatency[1].series[batch.batchId].value = batch.avgLatency / 4000;
    } else {
      this.throughputAndLatency[0].series.push({
        name: this.operatorLatestBatchNum['sl'].toString() + " batch",
        value: batch.throughput
      });
      this.throughputAndLatency[1].series.push({
        name: this.operatorLatestBatchNum['sl'].toString() + " batch",
        value: batch.avgLatency / 4000
      });
    }
    if (this.throughputAndLatency[0].series.length > 10) {
      this.throughputAndLatency[0].series.shift();
      this.throughputAndLatency[1].series.shift();
    }
    this.throughputAndLatency = this.throughputAndLatency.slice();
  }

  /**
   * Submit the batch form
   */
  submitBatchStatisticForm(): void {
    if (this.batchForm.valid) {
      this.jobInformationService.getBatchById(this.job.jobId, "sl", this.batchForm.controls.batch.value).subscribe(res => {
        if (res) {
          this.statisticBatch = res;
          this.updatePieChart(res);
          this.message.success(`Information of SLCombo Batch ${this.batchForm.controls.batch.value} is Fetched Successfully`);
        }
      });
    } else {
      Object.values(this.batchForm.controls).forEach(control => {
        if (control.invalid) {
          control.markAsDirty();
          control.updateValueAndValidity({onlySelf: true});
        }
      });
    }
  }

  submitTpgForm(): void {
    if (this.tpgForm.valid) {
      this.jobInformationService.getBatchById(this.job.jobId, "sl", this.tpgForm.controls.batch.value).subscribe(res => {
        if (res) {
          this.tpgBatch = res;
          this.tpgNodes = [];
          this.tpgLinks = [];
          this.numOfTD = 0;
          this.numOfLD = 0;
          this.numOfPD = 0;
          for (let node of res.tpg) {
            this.tpgNodes.push({name: node.operationID});
            for (let edge of node.edges) {
              this.tpgLinks.push({source: node.operationID, target: edge.dstOperatorID, type: edge.dependencyType});
              if (edge.dependencyType == "TD") {
                this.numOfTD++;
              } else if (edge.dependencyType == "LD") {
                  this.numOfLD++;
              } else {
                  this.numOfPD++;
              }
            }
          }
          this.tpgData = [{name: 'TD', value: this.numOfTD,}, {name: 'LD', value: this.numOfLD,}, {name: 'PD', value: this.numOfPD,}];
          this.message.success(`TPG of SLCombo Batch ${this.tpgForm.controls.batch.value} is Fetched Successfully`);
        }
      });
    } else {
      Object.values(this.tpgForm.controls).forEach(control => {
        if (control.invalid) {
          control.markAsDirty();
          control.updateValueAndValidity({onlySelf: true});
        }
      });
    }
  }

  /**
   * Update the pie chart
   * @param batch
   */
  updatePieChart(batch: Batch) {
    this.timePieData = [{
      name: 'overhead time time (ms)',
      value: batch.overallTimeBreakdown.overheadTime,
    },
      {
        name: 'stream time time (ms)',
        value: batch.overallTimeBreakdown.streamTime,
      },
      {
        name: 'transaction time (ms)',
        value: batch.overallTimeBreakdown.txnTime,
      }
    ];
    this.timePieData = this.timePieData.slice();
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
      .force('charge', d3.forceManyBody().strength(-15))
      .force('link', d3.forceLink(this.tpgLinks).id((d: any) => d.name))
      .force('center', d3.forceCenter(250, 200));

    this.linksSelections = this.tpgSvg.selectAll('.link')
      .data(this.tpgLinks)
      .enter().append('line')
      .attr('class', 'link')
      .style("stroke", (d: any) => {
        console.log(d)
        if (d.type == "FD") {
          return "#94A0C0"
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
    this.tpgSvgSimulation.alpha(0.3).restart();
    setInterval(() => {
      this.tpgSvgSimulation.stop();
      }, 2000);

    const tooltip = d3.select(this.tpgContainer.nativeElement)
        .append('div')
        .attr('class', 'tooltip')
        .style('height', '30px')
        .style('opacity', 0);

    this.nodesSelections.on('mouseover', (event, d) => {
      tooltip.transition()
          .duration(200)
          .style('opacity', 0.9);

      tooltip.html(`Transaction ID: ${d.name}`)
          .style('left', (event.pageX + 10) + 'px')
          .style('top', (event.pageY - 28) + 'px');
    });

    this.nodesSelections.on('mouseout', () => {
      tooltip.transition()
          .duration(500)
          .style('opacity', 0);
    });
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

  /**
   * Callback when user starts the job
   */
  onStart() {
    this.jobInformationService.startJob(this.job.jobId).subscribe(success => {
      if (success) {
        // start runtime-querying performance data
      }
    });
    this.jobStarted = true;
  }

  /**
   * Callback when user stops the job
   */
  onStop() {
    this.jobInformationService.stopJob(this.job.jobId).subscribe(success => {
      if (success) {
        // stop runtime-querying performance data
      }
    });
    this.jobStarted = false;
  }

  /**
   * Callback when the tpg graph is zoomed
   * @param transform
   */
  tpgZoomed({transform}) {
    const { k, x, y } = transform;
    this.tpgSvg.selectAll('.node').attr('transform', transform);
    this.tpgSvg.selectAll('.link').attr('transform', transform);
    // this.tpgSvg.selectAll('.tooltip').attr('transform', transform);
    this.tpgSvg.select('.tooltip').attr('transform', `scale(${1 / k})`);
  }

  /**
   * Callback when the tpg modal is expanded
   */
  onExpandTpgModal() {
    this.isTpgModalVisible = true;
    setTimeout(() => {
      this.drawTpgGraph();
    }, 1000);
  }

  /**
   * Callback when the tpg modal is closed
   */
  onClearTpgModal() {
    this.isTpgModalVisible = false;
    // this.drawTpgGraph();
  }
}
