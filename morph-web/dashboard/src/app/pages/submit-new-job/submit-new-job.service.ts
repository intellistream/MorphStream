import { Injectable } from '@angular/core';
import {Websocket} from "../../services/utils/websocket";
import {HttpClient} from "@angular/common/http";

@Injectable({
  providedIn: 'root'
})
export class SubmitNewJobService {
  constructor(private websocket: Websocket, private httpClient: HttpClient) {
    this.websocket.connect("ws://localhost:5001/websocket");
  }

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
