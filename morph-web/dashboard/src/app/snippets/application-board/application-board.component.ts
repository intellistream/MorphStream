import {Component, Input, OnInit} from '@angular/core';
import {Operator} from "../../model/Operator";
import {Job} from "../../model/Job";

@Component({
  selector: 'app-application-board',
  templateUrl: './application-board.component.html',
  styleUrls: ['./application-board.component.less']
})
export class ApplicationBoardComponent implements OnInit {
  @Input() application: Job;
  selectedOperator: Operator;

  onSelectedOperatorChange(operator: Operator) {
    this.selectedOperator = operator;
  }

  ngOnInit(): void {
    this.selectedOperator = this.application.operators[0];
  }
}
