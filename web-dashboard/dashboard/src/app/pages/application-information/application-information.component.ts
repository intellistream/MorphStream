import {AfterViewInit, Component, ElementRef, OnInit, ViewChild} from '@angular/core';
import {ApplicationService} from "../../shared/services/application.service";
import {Application} from "../../model/application";
import {WebsocketService} from "../../services/utils/websocket.service";

@Component({
  selector: 'app-application-information',
  templateUrl: './application-information.component.html',
  styleUrls: ['./application-information.component.less']
})
export class ApplicationInformationComponent implements OnInit, AfterViewInit {
  overallThroughput = (): string => `20.9 K tuple/sec`;
  processingLatency = (): string => `2230 ms`;

  @ViewChild('progressBarCol', { static: false }) progressBarCol!: ElementRef;
  progressBarWidth = 0; // the width of the progressbar

  application!: Application;

  constructor(private applicationService: ApplicationService, private websocketService: WebsocketService) {
  }

  ngAfterViewInit() {
    this.setProgressBarWidth();
  }

  setProgressBarWidth() {
    this.progressBarWidth = this.progressBarCol.nativeElement.offsetWidth;
  }

  ngOnInit(): void {
    this.application = this.applicationService.getCurrentApplication();

    this.websocketService.connect("ws://localhost:5001/websocket");

    this.websocketService.messageSubject.subscribe(data => {
        console.log(data);
    });

    this.websocketService.sendMessage("Hello World", function () {console.log("HAHA")});
  }
}
