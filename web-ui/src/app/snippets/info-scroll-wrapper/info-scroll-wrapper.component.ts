import {AfterViewInit, Component, ElementRef, Input, ViewChild} from '@angular/core';
import {Operator} from "../../model/operator";

@Component({
  selector: 'app-info-scroll-wrapper',
  templateUrl: './info-scroll-wrapper.component.html',
  styleUrls: ['./info-scroll-wrapper.component.less']
})
export class InfoScrollWrapperComponent implements AfterViewInit {
  @ViewChild('OperatorsContainer') scrollContainer!: ElementRef<HTMLElement>;
  @ViewChild('leftButton') leftButton!: ElementRef<HTMLButtonElement>;
  @ViewChild('rightButton') rightButton!: ElementRef<HTMLButtonElement>;
  @Input() operators: Operator[] = [];

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
}
