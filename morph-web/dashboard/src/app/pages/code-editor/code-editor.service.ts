import {HttpClient, HttpEvent, HttpEventType, HttpRequest, HttpResponse} from "@angular/common/http";
import {Injectable} from "@angular/core";
import {Observable, Observer} from "rxjs";
import {NzUploadFile} from "ng-zorro-antd/upload";

@Injectable({
  providedIn: 'root'
})
export class CodeEditorService {
  constructor(private http: HttpClient) {
  }

  public submitNewJobByConfigFile(job: string, parallelism: number, startNow: boolean, code: string, configFile: NzUploadFile): Observable<any> {
    const formData = new FormData();
    formData.append('jobName', job);
    formData.append('parallelism', parallelism.toString());
    formData.append('startNow', startNow.toString());
    formData.append('code', code);
    // @ts-ignore
    formData.append('configFile', configFile.originFileObj, configFile.name);

    return this.http.post(`http://localhost:8080/api/signal/submit/job_config`, formData);
  }

  public submitNewJobByDescription(job: string, parallelism: number, startNow: boolean, code: string, description: string): Observable<any> {
    const formData = new FormData();
    formData.append('jobName', job);
    formData.append('parallelism', parallelism.toString());
    formData.append('startNow', startNow.toString());
    formData.append('code', code);
    formData.append('description', description);

    return this.http.post(`http://localhost:8080/api/signal/submit/job_description`, formData);
  }

  public uploadConfigFile(item: any): Observable<any> {
    return new Observable((observer: Observer<any>) => {
      const formData = new FormData();
      formData.append('file', item.file as any);
      const req = new HttpRequest('POST', 'http://localhost:8080/api/signal/submit/config', formData, {
        reportProgress: true,
      });

      this.http.request(req).subscribe({
        next: (event: HttpEvent<any>) => {
          if (event.type === HttpEventType.UploadProgress) {
            const percentDone = Math.round(100 * event.loaded / (event.total ?? 1));
            observer.next({percent: percentDone});
          } else if (event instanceof HttpResponse) {
            observer.complete();
          }
        },
        error: (err) => {
          observer.error(err);  // Upload failed
        },
        complete: () => {
          observer.complete();
        }
      });
    });
  }
}
