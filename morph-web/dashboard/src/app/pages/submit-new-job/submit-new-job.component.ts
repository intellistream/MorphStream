import { Component } from '@angular/core';
import {NzUploadFile} from "ng-zorro-antd/upload";
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
    this.isVisible = false;
  }

  handleOk(): void {
    this.isVisible = false;
  }

  onAddNew(): void {
    this.isVisible = true;
  }

  // handleUpload = (item: any) => {
  //   const formData = new FormData();
  //   formData.append(item.name, item.file as any);
  //
  //   return this.submitNewJobService.uploadNewJob().subscribe(res => {
  //       console.log("success", res.id);
  //     }
  //   );
  // }
}
