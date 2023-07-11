import { Component } from '@angular/core';
import {Router} from "@angular/router";

@Component({
  selector: 'app-application-information',
  templateUrl: './application-information.component.html',
  styleUrls: ['./application-information.component.less']
})
export class ApplicationInformationComponent {
  overallThroughput = (): string => `20.9 K tuple/sec`;
  processingLatency = (): string => `2230 ms`;

  constructor(private router: Router) {
  }
}
