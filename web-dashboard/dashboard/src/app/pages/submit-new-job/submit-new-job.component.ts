import { Component } from '@angular/core';

@Component({
  selector: 'app-submit-new-job',
  templateUrl: './submit-new-job.component.html',
  styleUrls: ['./submit-new-job.component.less']
})
export class SubmitNewJobComponent {
  listOfData: any[] =[];
  isVisible = false;

  handleCancel(): void {
    console.log('Button cancel clicked!');
    this.isVisible = false;
  }

  handleOk(): void {
    console.log('Button ok clicked!');
    this.isVisible = false;
  }

  onAddNew(): void {
    this.isVisible = true;
  }
}
