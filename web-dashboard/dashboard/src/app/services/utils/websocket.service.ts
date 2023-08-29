import {Injectable} from '@angular/core';
import {interval, Subject} from 'rxjs';

/**
 * WebsocketService provides APIs about communications with backend
 */
@Injectable({
  providedIn: 'root'
})
export class WebsocketService {
  messageSubject: Subject<any>;                            // subject -> send messages
  private url;                                             // default requested url
  private webSocket: WebSocket;                            // websocket
  connectSuccess = false;                         // websocket connected signifier
  heartbeatPeriod = 60 * 1000 * 10;               // Heartbeat Check Period
  serverTimeoutSubscription;                               // Timeout checker
  reconnectFlag = false;                          // Reconnect
  reconnectPeriod = 5 * 1000;                     // Reconnect Period
  reconnectSubscription;                                   // Reconnect to Subscription Object
  runTimeSubscription;                                     // Record subscription
  runTimePeriod = 60 * 10000;                     // Record subscription time

  constructor() {
    this.messageSubject = new Subject();
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
  sendMessage(message, callback) {
    this.waitForConnection(function (websocket) {
      websocket.send(message);
      if (typeof callback !== "undefined") {
        callback();
      }
    }, 1000);
  }

  /**
   * Create a new connection
   */
  connect(url) {
    if (!!url) {
      this.url = url;
    }
    this.createWebSocket();
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
  onMessage(event) {
    console.log("New message received", event.data);
    const message = JSON.parse(event.data);
    console.log("Message received time: ", new Date().getTime());
    return message;
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
