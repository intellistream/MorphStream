import {Injectable} from '@angular/core';
import {interval, Subject} from 'rxjs';
import { v4 as uuidv4 } from 'uuid';// import {uuid} from 'uuidv4'

/**
 * WebsocketService provides APIs about communications with backend
 */
@Injectable({
  providedIn: 'root'
})
export class Websocket {
  consistentSubject: Subject<any>;                            // subject -> send messages
  url;                                                     // default requested url
  webSocket: WebSocket;                                    // websocket
  connectSuccess = false;                         // websocket connected signifier
  heartbeatPeriod = 60 * 1000 * 10;               // Heartbeat Check Period
  serverTimeoutSubscription;                               // Timeout checker
  reconnectFlag = false;                          // Reconnect
  reconnectPeriod = 5 * 1000;                     // Reconnect Period
  reconnectSubscription;                                   // Reconnect to Subscription Object
  runTimeSubscription;                                     // Record subscription
  runTimePeriod = 60 * 10000;                     // Record subscription time

  constructor() {
    this.consistentSubject = new Subject();
    this.startHeartbeat();
    this.calcRunTime();
  }

  /**
   * Wait for connection while sending a message
   */
  waitForConnection(callback, interval) {
    if (this.webSocket.readyState === 1) {
      // in ready state
      callback(this.webSocket);
    } else {
      let that = this;
      setTimeout(function () {
        that.waitForConnection(callback, interval);
      })
    }
  }

  /**
   * Send a message to backend
   */
  sendMessageWithCallback(message, callback) {
    this.waitForConnection(function (websocket) {
      websocket.send(message);
      if (typeof callback !== "undefined") {
        callback();
      }
    }, 1000);
  }

  sendMessage(message) {
    this.sendMessageWithCallback(message, undefined);
  }

  /**
   * Send a request and expecting a response
   */
  sendRequest<T>(requestMsg) {
    // register a new uuid
    let correlationId = uuidv4();
    requestMsg.correlationId = correlationId; // bind a correlationId to this msg
    this.sendMessage(JSON.stringify(requestMsg)); // send message

    const responseSubject = new Subject<T>();
    this.subjectMap.set(correlationId, responseSubject);
    return responseSubject.asObservable();
  }

  // sendUpload<T>(uploadMsg) {
  //   let correlationId = uuidv4();
  //   uploadMsg.correlationId = correlationId;
  //
  //   this.sendMessage(JSON.stringify(uploadMsg));
  //
  //   const responseSubject = new Subject<T>();
  //   this.subjectMap.set(correlationId, responseSubject);
  //   return responseSubject.asObservable();
  //
  // }

  /**
   * Create a new connection
   */
  connect(url) {
    if (!!url) {
      this.url = url;
    }
    this.createWebSocket();
  }

  jobId: number;

  listenOnJobData(jobId: number) {
    this.jobId = jobId;
    return this.consistentSubject.asObservable();
  }

  /**
   * Create a new websocket
   */
  createWebSocket() {
    this.webSocket = new WebSocket(this.url);
    // On socket open
    this.webSocket.onopen = (e) => this.onOpen(e);
    // On message receive
    this.webSocket.onmessage = (e) => this.onMessage(e);
    // On close
    this.webSocket.onclose = (e) => this.onClose(e);
    // On error
    this.webSocket.onerror = (e) => this.onError(e);
  }

  /**
   * Connection opened
   */
  onOpen(e) {
    console.log("websocket successfully connected...");
    this.connectSuccess = true;
    // check if it is a reconnection
    if (this.reconnectFlag) {
      this.stopReconnect();
      this.startHeartbeat();
      this.calcRunTime();
    }
  }

  /**
   * Message received
   */
  // onMessage(msg) {
  //   const message = JSON.parse(msg.data);
  //   const key = message.key;
  //
  //   if (this.subscriptions.get(key)) {
  //     this.subscriptions.get(key)!.next(message);
  //     this.deleteKey(msg.key);
  //   }
  //
  //   console.log("New message received: ", + message);
  //   return message;
  // }

  private subjectMap: Map<string, Subject<any>> = new Map();

  onMessage(msg) {
    // console.log("New message received: ", msg);
    const message = JSON.parse(msg.data); // parse the received message
    console.log(message);
    // check the type of the message => response |
    if (message.type === "response") {
      const correlationId: string = message.correlationId;
      // find the corresponding subject
      if (this.subjectMap.has(correlationId)) {
        const subject = this.subjectMap.get(correlationId);

        subject?.next(message.data);
        subject?.complete(); // subject is finished, cancel subscribe
        this.subjectMap.delete(correlationId);
      }
    } else if (message.type === "performance") {
      // pass message to messageSubject
      if (message.jobId == this.jobId) {
        this.consistentSubject.next(message);
      }
    }
  }

  /**
   * Close connection
   */
  private onClose(e) {
    console.log("Connection closed...", e);
    this.connectSuccess = false;
    this.webSocket.close();
    // 关闭时开始重连
    this.reconnect();
    this.stopRunTime();
    // throw new Error('webSocket connection closed:)');
  }

  /**
   * Connection error
   */
  private onError(e) {
    console.log('Connection error...', e);
    this.connectSuccess = false;
  }

  /**
   * Reconnect
   */
  reconnect() {
    if (this.connectSuccess) {
      this.stopReconnect();
      console.log("Reconnected...");
      return;
    }
    if (this.reconnectFlag) {
      console.log("Reconnecting in process...");
      return;
    }
    // start reconnecting
    this.reconnectFlag = true;

    // subscribe a reconnection process
    this.reconnectSubscription = interval(this.reconnectPeriod).subscribe(async (val) => {
      console.log(`Reconnect:${val} time`);
      const url = this.url;
      // Reconnect
      this.connect(url);
    });
  }

  /**
   * Stop reconnection
   */
  stopReconnect() {
    this.reconnectFlag = false;
    // unsubscribe reconnection
    if (typeof this.reconnectSubscription !== "undefined" && this.reconnectSubscription != null) {
      this.reconnectSubscription.unsubscribe();
    }
  }

  startHeartbeat() {
    console.log("Heartbeat detection starts...");
    this.serverTimeoutSubscription = interval(this.heartbeatPeriod).subscribe((val) => {
      if (this.webSocket != null && this.webSocket.readyState === 1) {
        console.log(val, "Stay connected...");
      } else {
        // stop heartbeat
        this.stopHeartbeat();
        // start reconnecting
        this.reconnect();
      }
    });
  }

  /**
   * Stop heartbeat check
   */
  stopHeartbeat() {
    // unsubscribe heartbeat check
    if (typeof this.serverTimeoutSubscription !== 'undefined' && this.serverTimeoutSubscription != null) {
      this.serverTimeoutSubscription.unsubscribe();
    }
  }

  /**
   * Calculate connection rum time
   */
  calcRunTime() {
    this.runTimeSubscription = interval(this.runTimePeriod).subscribe(period => {
      console.log("Connection runtime:", `${period} minutes`);
    });
  }

  /**
   * Stop calculating run time
   */
  stopRunTime() {
    if (typeof this.runTimeSubscription !== 'undefined' && this.runTimeSubscription !== null) {
      this.runTimeSubscription.unsubscribe();
    }
  }
}
