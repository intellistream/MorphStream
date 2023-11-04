import { Injectable } from '@angular/core';
import {HttpClient} from "@angular/common/http";

@Injectable({
  providedIn: 'root'
})
export class SubmitNewJobService {
  constructor(private httpClient: HttpClient) { }

  //
  // public uploadNewJob(): Observable<any> {
  //   let msg = {
  //     "type": "UploadJar",
  //     "correlationId": ""
  //   }
  //
  //   return this.websocket.sendUpload(msg);
  // }
}
