import {Component, Input} from '@angular/core';
import {Operator} from "../../model/Operator";

@Component({
  selector: 'app-operator-information',
  templateUrl: './operator-information.component.html',
  styleUrls: ['./operator-information.component.less']
})
export class OperatorInformationComponent {
  @Input() operator: Operator = {
    id: 2,
    name: "Tweet Registrant",
    numOfInstances: 4,
    throughput: 27.8, // tuples/s
    latency: 345.4,  // ms
    explorationStrategy: "Structured Exploration",
    schedulingGranularity: "Fine-Grained Unit",
    abortHandling: "Eager Abort",
    numOfTD: 632,
    numOfLD: 450,
    numOfPD: 120
  }

  constructor() {
  }

  // onTpgClick() {
  //   this.modalService.create({
  //     nzTitle: `TPG of ${this.operator.name}`,
  //     nzContent: TpgGraphComponent,
  //     nzFooter: null,
  //     nzWidth: '1248px',
  //     nzBodyStyle: {height: "748px"},
  //     nzCentered: true
  //   });
  // }
}
