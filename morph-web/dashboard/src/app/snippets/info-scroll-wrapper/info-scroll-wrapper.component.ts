import {AfterViewInit, Component, ElementRef, EventEmitter, Input, Output, ViewChild} from '@angular/core';
import {Operator} from "../../model/Operator";

@Component({
  selector: 'app-info-scroll-wrapper',
  templateUrl: './info-scroll-wrapper.component.html',
  styleUrls: ['./info-scroll-wrapper.component.less']
})
export class InfoScrollWrapperComponent implements AfterViewInit {
  @ViewChild('OperatorsContainer') scrollContainer!: ElementRef<HTMLElement>;
  @ViewChild('leftButton') leftButton!: ElementRef<HTMLButtonElement>;
  @ViewChild('rightButton') rightButton!: ElementRef<HTMLButtonElement>;
  @Input() operators: Operator[] = [
    {
      id: 1,
      name: "Spout",
      numOfInstances: 1,
      throughput: -1, // tuples/s
      latency: -1,  // ms
      explorationStrategy: "NA",
      schedulingGranularity: "NA",
      abortHandling: "NA",
      numOfTD: -1,
      numOfLD: -1,
      numOfPD: -1
    },
    {
      id: 2,
      name: "Tweet Registrant",
      numOfInstances: 4,
      throughput: 27.8, // tuples/s
      latency: 345.4,  // ms
      explorationStrategy: "Structured Exploration",
      schedulingGranularity: "Fine-Grained Unit",
      abortHandling: "Eager Abort",
      numOfTD: 120,
      numOfLD: 560,
      numOfPD: 80
    },
    {
      id: 3,
      name: "Word Updater",
      numOfInstances: 4,
      throughput: 21.3, // tuples/s
      latency: 438.8,  // ms
      explorationStrategy: "Non-Structured Exploration",
      schedulingGranularity: "Fine-Grained Unit",
      abortHandling: "Eager Abort",
      numOfTD: 588,
      numOfLD: 790,
      numOfPD: 422
    },
    {
      id: 4,
      name: "Trend Calculator",
      numOfInstances: 4,
      throughput: 24.9, // tuples/s
      latency: 266.3,  // ms
      explorationStrategy: "Structured Exploration",
      schedulingGranularity: "Fine-Grained Unit",
      abortHandling: "Lazy Abort",
      numOfTD: 632,
      numOfLD: 450,
      numOfPD: 120
    }
  ];
  selectedOperator: Operator = this.operators[0];

  ngAfterViewInit() {
    this.checkScrolling();
  }

  scrollLeft() {
    const container = this.scrollContainer.nativeElement;
    const currentScrollPosition = container.scrollLeft;
    const scrollAmount = 800;

    container.scrollTo({
      left: currentScrollPosition - scrollAmount,
      behavior: 'smooth'
    });
  }

  scrollRight() {
    const container = this.scrollContainer.nativeElement;
    const currentScrollPosition = container.scrollLeft;
    const scrollAmount = 800;

    container.scrollTo({
      left: currentScrollPosition + scrollAmount,
      behavior: 'smooth'
    });
  }

  private checkScrolling() {
    // Hide the left & right buttons if scrolling is not necessary
    const container = this.scrollContainer.nativeElement;
    const leftButton = this.leftButton.nativeElement;
    const rightButton = this.rightButton.nativeElement;

    if (container.scrollWidth <= container.clientWidth) {
      leftButton.style.visibility = 'hidden';
      rightButton.style.visibility = 'hidden';
    } else {
      leftButton.style.visibility = 'visible';
      rightButton.style.visibility = 'visible';
    }
  }

  @Output() selectedOperatorChange = new EventEmitter<Operator>();

  public onOperatorClick(operator: Operator) {
    this.selectedOperator = operator;
    this.selectedOperatorChange.emit(operator);
  }
}
