import { Component } from '@angular/core';
import {NzUploadFile} from "ng-zorro-antd/upload";
import {Observable} from "rxjs";
import {SubmitNewJobService} from "./submit-new-job.service";

@Component({
  selector: 'app-submit-new-job',
  templateUrl: './submit-new-job.component.html',
  styleUrls: ['./submit-new-job.component.less']
})
export class SubmitNewJobComponent {
  listOfData: any[] =[];
  isVisible = false;
  fileList: NzUploadFile[] = [];

  constructor(private submitNewJobService: SubmitNewJobService) {
  }

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

  // handleUpload = (item: any) => {
  //   const formData = new FormData();
  //   formData.append(item.name, item.file as any);
  //
  //   console.log(formData);
  //
  //   return this.submitNewJobService.uploadNewJob().subscribe(res => {
  //       console.log("success", res.id);
  //     }
  //   );
  // }
}
