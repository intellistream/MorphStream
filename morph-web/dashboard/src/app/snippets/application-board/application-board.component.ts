import {Component, Input, OnInit} from '@angular/core';
import {Operator} from "../../model/Operator";
import {Application} from "../../model/Application";

@Component({
  selector: 'app-application-board',
  templateUrl: './application-board.component.html',
  styleUrls: ['./application-board.component.less']
})
export class ApplicationBoardComponent implements OnInit {
  @Input() application: Application;
  selectedOperator: Operator;

  onSelectedOperatorChange(operator: Operator) {
    this.selectedOperator = operator;
  }

  ngOnInit(): void {
    this.selectedOperator = this.application.operators[0];
  }
}
