import {Component, Input} from '@angular/core';
import {Application} from "../../model/application";

@Component({
  selector: 'app-application-board',
  templateUrl: './application-board.component.html',
  styleUrls: ['./application-board.component.less']
})
export class ApplicationBoardComponent {
  @Input() application: Application = {
    id: 1,
    name: "Online Social Media Event Detection",
    numOfThreads: 8,
    cpu: "Intel(R) Xeon(R) Silver 4310 CPU @2.10GHz",
    ram: "32GB",
    startTime: "2023-Mar-10 09:15:27",
    duration: "00:08:17",
    isRunning: true
  }

  // @Input() boardWidth: string = "780px";
  // @Input()  boardHeight: string = "270px";
}
