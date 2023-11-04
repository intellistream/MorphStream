import {HttpClient} from "@angular/common/http";
import {Injectable} from "@angular/core";

@Injectable({
  providedIn: 'root'
})
export class CodeEditorService {
  constructor(private http: HttpClient) {}

  public submitNewJob(job: string, parallelism: number, startNow: boolean, code: string) {
    return this.http.post(`http://localhost:8080/api/signal/submit`, {
      job,
      parallelism,
      startNow,
      code
    });
  }
}
