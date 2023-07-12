import {AfterViewInit, Component, ElementRef, ViewChild} from '@angular/core';
import {Router} from "@angular/router";

@Component({
  selector: 'app-application-information',
  templateUrl: './application-information.component.html',
  styleUrls: ['./application-information.component.less']
})
export class ApplicationInformationComponent implements AfterViewInit {
  overallThroughput = (): string => `20.9 K tuple/sec`;
  processingLatency = (): string => `2230 ms`;

  @ViewChild('progressBarCol', { static: false }) progressBarCol!: ElementRef;
  progressBarWidth = 0; // the width of the progressbar

  @ViewChild('cardRef', { static: false }) cardRef!: ElementRef;
  appCardWidth = "";

  constructor(private router: Router) {
  }

  ngAfterViewInit() {
    this.setProgressBarWidth();
    this.setCardWidth();
  }

  setProgressBarWidth() {
    this.progressBarWidth = this.progressBarCol.nativeElement.offsetWidth;
  }
  setCardWidth() {
    this.appCardWidth = this.cardRef.nativeElement.offsetWidth +"px";
  }
}
