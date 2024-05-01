import {Component, ElementRef, OnInit, ViewChild} from '@angular/core';

import {JobInformationService} from "./job-information.service";
import {Job} from "../../dto/Job";
import {ActivatedRoute, Router} from "@angular/router";

import * as d3 from 'd3';
import {
  NzGraphComponent,
  NzGraphData,
  NzGraphDataDef,
  NzGraphZoomDirective,
} from "ng-zorro-antd/graph";
import {Batch} from "../../dto/Batch";
import {FormControl, FormGroup, NonNullableFormBuilder, Validators} from "@angular/forms";
import {NzMessageService} from "ng-zorro-antd/message";
import {NzModalService} from "ng-zorro-antd/modal";

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

  operatorGraphData: NzGraphDataDef = {nodes: [], edges: []} // operator graph data {v: '1', w: '2'}, {id: '101', label: 'Spout'}
  nzOperatorGraphData: any; // operator graph data for nz-graph

  // Batch data
  batchOptions: any[] = [];
  throughputAndLatency: any[] = [
    {name: 'Throughput (x1000 tuples/s)', series: []},
    {name: 'Latency (ms)', series: []}
  ];
  onShowingThroughputAndLatency: any[] = [
    {name: 'Throughput (x1000 tuples/s)', series: []},
    {name: 'Latency (ms)', series: []}
  ];

  timePieData: any[] = [{name: 'construct time (ns)', value: 0,},
    {name: 'explore time (ns)', value: 0,},
    {name: 'useful time (ns)', value: 0,},
    {name: 'abort time (ns)', value: 0}];
  tpgData: any[] = [{name: 'TD', value: 0,}, {name: 'LD', value: 0,}, {name: 'PD', value: 0,}];

  operatorLatestBatchNum: { [key: string]: number } = {} // key: operator name, value: latest batch
  operatorAccumulativeLatency: { [key: string]: number } = {} // key: operator name, value: accumulative latency
  operatorAccumulativeThroughput: { [key: string]: number } = {} // key: operator name, value: accumulative throughput

  // batch-tpg data
  tpgNodes: any[] = [];
  tpgLinks: any[] = [];
  numOfTD = 0;
  numOfLD = 0;
  numOfPD = 0;

  nodesSelections: any;
  linksSelections: any;

  jobStarted = false;
  throughputLatencyGraphSize = 10;
  throughputLatencyStartBatch = 1;
  realTimePerformanceBoxChecked = false;
  startBatchOptionDisabled = false;

  batchForm: FormGroup<{
    batch: FormControl<string>;
    operator: FormControl<string>;
  }>;

  tpgForm: FormGroup<{
    batch: FormControl<string>;
    operator: FormControl<string>;
  }>;

  runtimeDuration = 0;
  listenIntervalId: number;     // interval id for listening to the performance data

  constructor(private route: ActivatedRoute,
              private jobInformationService: JobInformationService,
              private fb: NonNullableFormBuilder,
              private message: NzMessageService,
              private modal: NzModalService,
              private router: Router) {
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
        // add spout to operator graph
        this.operatorGraphData.nodes.push({id: "spout", label: "Spout", instance: 1});
        // add operators to operator graph
        for (let i = 0; i < this.job.operators.length; i++) {
          this.operatorLatestBatchNum["sl"] = 1;  // initialize the latest batch number
          this.operatorAccumulativeLatency["sl"] = 0;  // initialize the accumulative latency
          this.operatorAccumulativeThroughput["sl"] = 0;  // initialize the accumulative throughput
          this.operatorGraphData.nodes.push({id: this.job.operators[i].id, label: this.job.operators[i].name, instance: this.job.operators[i].numOfInstances});
          if (i > 0) {
            this.operatorGraphData.edges.push({v: this.job.operators[i - 1].id, w: this.job.operators[i].id});
          }
        }
        // add sink to operator graph
        this.operatorGraphData.nodes.push({id: "sink", label: "Sink", instance: 1});
        this.operatorGraphData.edges.push({v: this.job.operators[this.job.operators.length - 1].id, w: "sink"});
        this.operatorGraphData.edges.push({v: "spout", w: this.job.operators[0].id});
        this.drawOperatorGraph();
        this.getHistoricalData();

        if (this.jobStarted) {
          // start runtime-querying performance data
          this.startListening();
        }
      });
    });
  }

  /**
   * Get historical data of the job
   */
  getHistoricalData() {
    this.jobInformationService.getAllBatches(this.job.jobId, 'sl').subscribe(res => {
      res.sort((a, b) => {
        return a.batchId - b.batchId;
      });
      this.operatorLatestBatchNum['sl'] = res.length + 1;
      for (let batch of res) {
        batch = this.transformTime(batch);
        this.batchOptions.push({
          value: batch.batchId.toString(),
          label: batch.batchId.toString()
        });
        this.throughputAndLatency[0].series.push({name: `batch${batch.batchId}`, value: parseFloat((batch.throughput / 10 ** 3).toFixed(1))});
        this.throughputAndLatency[1].series.push({name: `batch${batch.batchId}`, value: batch.avgLatency});
        this.operatorAccumulativeLatency['sl'] = batch.accumulativeLatency;
        this.operatorAccumulativeThroughput['sl'] = batch.accumulativeThroughput;
        this.runtimeDuration += batch.batchDuration;
        this.updateOnShowingThroughputAndLatency();
      }
      this.runtimeDuration = parseFloat((this.runtimeDuration / 10**9).toFixed(1)); // s
    });
  }

  /**
   * Update the throughput and latency data
   */
  updateOnShowingThroughputAndLatency() {
    if (this.realTimePerformanceBoxChecked) {
      if (this.operatorLatestBatchNum['sl'] - this.throughputLatencyGraphSize + 1 > 0) {
        this.throughputLatencyStartBatch = this.operatorLatestBatchNum['sl'] - this.throughputLatencyGraphSize + 1;
        this.onShowingThroughputAndLatency[0].series = this.throughputAndLatency[0].series.slice(this.throughputLatencyStartBatch - 1, this.throughputLatencyStartBatch + this.throughputLatencyGraphSize - 1);
        this.onShowingThroughputAndLatency[1].series = this.throughputAndLatency[1].series.slice(this.throughputLatencyStartBatch - 1, this.throughputLatencyStartBatch + this.throughputLatencyGraphSize - 1);
      } else {
        this.throughputLatencyStartBatch = 1;
        this.onShowingThroughputAndLatency[0].series = this.throughputAndLatency[0].series.slice(0, this.throughputLatencyGraphSize);
        this.onShowingThroughputAndLatency[1].series = this.throughputAndLatency[1].series.slice(0, this.throughputLatencyGraphSize);
      }
    } else {
      this.onShowingThroughputAndLatency[0].series = this.throughputAndLatency[0].series.slice(this.throughputLatencyStartBatch - 1, this.throughputLatencyStartBatch + this.throughputLatencyGraphSize - 1);
      this.onShowingThroughputAndLatency[1].series = this.throughputAndLatency[1].series.slice(this.throughputLatencyStartBatch - 1, this.throughputLatencyStartBatch + this.throughputLatencyGraphSize - 1);
    }
    this.onShowingThroughputAndLatency = this.onShowingThroughputAndLatency.slice();
  }

  /**
   * Start listening to the performance data
   */
  startListening() {
    this.listenIntervalId = setInterval(() => {
      this.update();
      this.runtimeDuration = parseFloat((this.runtimeDuration + 1).toFixed(1));  // seconds
    }, 1000);
  }

  /**
   * Update the runtime data
   */
  update() {
    this.jobInformationService.getBatchById(this.job.jobId, 'sl', this.operatorLatestBatchNum['sl'].toString()).subscribe(res => {
      if (res) {
        res = this.transformTime(res);
        this.batchOptions.push({
          value: this.operatorLatestBatchNum['sl'].toString(),
          label: this.operatorLatestBatchNum['sl'].toString()
        });
        this.updatePerformanceGraph(res);
        this.operatorLatestBatchNum['sl']++;  // update the latest batch number
        this.operatorAccumulativeLatency['sl'] = res.accumulativeLatency;  // update the accumulative latency
        this.operatorAccumulativeThroughput['sl'] = res.accumulativeThroughput;  // update the accumulative throughput
      }
    });
  }

  /**
   * Transform the time unit
   * @param batch
   */
  transformTime(batch: Batch): Batch {
    batch.throughput = parseFloat(batch.throughput.toFixed(1)); // tuples/s
    batch.avgLatency = parseFloat((batch.avgLatency / 10**6).toFixed(1)); // ms
    batch.minLatency = parseFloat((batch.minLatency / 10**6).toFixed(1)); // ms
    batch.maxLatency = parseFloat((batch.maxLatency / 10**6).toFixed(1)); // ms
    batch.accumulativeLatency = parseFloat((batch.accumulativeLatency / 10**6).toFixed(1)); // ms
    batch.accumulativeThroughput = parseFloat(batch.accumulativeThroughput.toFixed(1)); // tuples/s
    return batch;
  }

  /**
   * Update the performance graph
   */
  updatePerformanceGraph(batch: Batch) {
    this.throughputAndLatency[0].series.push({
      name: this.operatorLatestBatchNum['sl'].toString() + " batch",
      value: parseFloat((batch.throughput / 10 ** 3).toFixed(1))  // 1000 tuples/s
    });
    this.throughputAndLatency[1].series.push({
      name: this.operatorLatestBatchNum['sl'].toString() + " batch",
      value: batch.avgLatency
    });
    this.updateOnShowingThroughputAndLatency();
  }

  /**
   * Submit the batch form
   */
  submitBatchStatisticForm(): void {
    if (this.batchForm.valid) {
      this.jobInformationService.getBatchById(this.job.jobId, "sl", this.batchForm.controls.batch.value).subscribe(res => {
        if (res) {
          res = this.transformTime(res);
          this.statisticBatch = res;
          this.statisticBatch.batchDuration = parseFloat((this.statisticBatch.batchDuration / 10**6).toFixed(1)); // ms
          this.updatePieChart(res);
          this.message.success(`Information of SL Batch ${this.batchForm.controls.batch.value} is Fetched Successfully`);
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
            this.tpgNodes.push(node);
            for (let edge of node.edges) {
              if (edge.dstOperatorID != edge.srcOperatorID) {
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
          }
          this.tpgData = [{name: 'TD', value: this.numOfTD,}, {name: 'LD', value: this.numOfLD,}, {name: 'PD', value: this.numOfPD,}];
          this.message.success(`TPG of SL batch ${this.tpgForm.controls.batch.value} is fetched successfully`);
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
      name: 'construct time (ns)',
      value: batch.schedulerTimeBreakdown.constructTime // ns
    },
      {
        name: 'explore time (ns)',
        value: batch.schedulerTimeBreakdown.exploreTime // ns
      },
      {
        name: 'useful time (ns)',
        value: batch.schedulerTimeBreakdown.usefulTime // ns
      },
      {
        name: 'abort time (ns)',
        value: batch.schedulerTimeBreakdown.abortTime // ns
      }
    ];
    this.timePieData = this.timePieData.slice();
  }

  /**
   * Draw the operator graph
   */
  drawOperatorGraph() {
    this.nzOperatorGraphData = new NzGraphData(this.operatorGraphData);
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
      .force('link', d3.forceLink(this.tpgLinks).id((d: any) => d.operationID))
      .force('center', d3.forceCenter(250, 250));

    this.linksSelections = this.tpgSvg.selectAll('.link')
      .data(this.tpgLinks)
      .enter().append('line')
      .attr('class', 'link')
      .style("stroke", (d: any) => {
        if (d.type == "FD") {
          return "#A37EA9"
        } else if (d.type == "LD") {
          return "#7DA3E4"
        } else {
          // TODO: fix the issue of PD and LD overlap
          if (Math.random()>0.3) {
            return "#A93B5E"
          } else {
            return "#7DA3E4"

          }        }
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
    setTimeout(() => {
      this.tpgSvgSimulation.stop();
      }, 4000);

    // add tooltip
    const tooltip = d3.select(this.tpgContainer.nativeElement)
        .append('div')
        .attr('class', 'tooltip')
        .style('height', '30px')
        .style('opacity', 0);

    this.nodesSelections.on('mouseover', (event, d) => {
      tooltip.transition()
          .duration(200)
          .style('opacity', 0.9);

      tooltip.html(`transaction id: ${d.operationID}&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;transaction type: ${d.txnType}&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;targetTable: ${d.targetTable}&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;targetKey: ${d.targetKey}`)
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
    this.jobStarted = true;
    this.startListening();  // start runtime-querying performance data
    this.jobInformationService.startJob(this.job.jobId).subscribe(success => {
      if (success) {
        console.log("start job successfully");
      }
    });
  }

  /**
   * Callback when user stops the job
   */
  onStop() {
    if (this.realTimePerformanceBoxChecked) {
      this.onCheckBoxChanged(false);
    }
    clearInterval(this.listenIntervalId);  // stop runtime-querying performance data
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
    const tpgLoading = this.message.loading(`Loading TPG: ${this.tpgForm.value.operator} batch ${this.tpgForm.value.batch} in progress..`, { nzDuration: 0 }).messageId;
    setTimeout(() => {
      this.message.remove(tpgLoading);
    }, 1000);
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
  }

  /**
   * Callback when the performance graph x-axis size & start batch is changed
   * @param event
   */
  onPerformanceGraphConfigChange(event: any) {
    this.updateOnShowingThroughputAndLatency();
  }

  /**
   * Callback when the real-time performance box is checked
   * @param newValue
   */
  onCheckBoxChanged(newValue: boolean) {
    this.realTimePerformanceBoxChecked = newValue;
    this.startBatchOptionDisabled = newValue;
  }

  onDeleteTrigger() {
    this.modal.confirm({
      nzTitle: 'Are you sure to delete this job?',
      nzContent: '<b>This operation cannot be undone.</b>',
      nzOkText: 'Yes',
      nzOkType: 'primary',
      nzOkDanger: true,
      nzCancelText: 'No',
      nzStyle: {'top': '30%'}, // style="top: 20%"
      nzOnOk: () => {
        this.onDelete();
      },
      nzOnCancel: () => {}
    })
  }

  /**
   * Callback when the job is deleted
   */
  onDelete() {
    this.jobInformationService.deleteJob(this.job.jobId).subscribe(success => {
      if (success) {
        console.log("delete job successfully");
        this.message.success("Delete Job Successfully");
        // redirect to the overview page
        this.router.navigate(['/overview']);
      }
    });
  }
}
