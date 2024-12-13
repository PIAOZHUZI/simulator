const cluster = require("cluster");

const mqtt = require("mqtt");

const net = require("net");

var mqtt_host = process.env.THINGPLUG_HOST1 || 'localhost';

var http_host = process.env.THINGPLUG_HTTP_HOST || mqtt_host;

const EventEmitter = require("events");


let numConn = 0
let sendTelemetryCount  = 0
let recvReqCount   = 0
let sendReqCount  = 0
let inReconnectDeviceCount = 0




var http_port = 9000;
var mqtt_port = 1883;
var portIdx = 10000;

const count = 49; // 원하는 난수 개수 (0.01부터 0.99까지의 숫자이므로 99개)
const min = 10; // 최소값 (0.01 * 100)
const max = 59; // 최대값 (0.99 * 100)
var rcount =0;
var rr = 0;
console.log = function() {				 

} 

function generateUniqueRandomNumbers(count, min, max) {
  
	if (count > (max - min + 1)) {
		throw new Error("Count should be less than or equal to the range of numbers.");
	}

	const uniqueNumbers = new Set();
	const randomNumbers = [];

	while (uniqueNumbers.size < count) {
		const randomNumber = Math.floor(Math.random() * (max - min + 1)) + min;
		if (!uniqueNumbers.has(randomNumber)) {
		  uniqueNumbers.add(randomNumber);
		  randomNumbers.push(randomNumber);
		}
	}
	return randomNumbers;
}

function getPayload() {

  var temperature = 10 + Math.round(Math.random() * 20);

  var payload = {

    "temperature": temperature,

  };

  return payload;

}




class Device {

  constructor(serviceId, deviceId, deviceToken, connectAfter, telemetryInterval, localIp, deadInterval, reconnectInterval, numDevice, rds) {

    this.requestTopic = "v1/dev/" + serviceId + "/" + deviceId + "/up";

    this.responseTopic = "v1/dev/" + serviceId + "/" + deviceId + "/down";

    this.telemetryTopic = "v1/dev/" + serviceId + "/" + deviceId + "/telemetry";

    this.deviceToken = deviceToken;

    this.telemetryInterval = telemetryInterval;

    this.connectAfter = connectAfter;

    this.localIp = localIp;

    this.connected = false;

    this.isRecon = false;

    this.isDead = false;

    this.reconnectInterval = reconnectInterval;

    this.MAX_NUM = numDevice;
	
	this.rds= rds;
	
	this.randomNumbers = generateUniqueRandomNumbers(count, min, max);

  }




  connect() {

    var self = this;

    if (self.isRecon == true) {

        self.client = null;

    }

	if(count== rcount){
		rcount = 0;
	}


    if (self.client == null && !self.connected) {

        var option = {

			username: this.deviceToken,

			clean: true,

			clientId: this.deviceToken,

            keepalive: 120
            
        };




        self.client = new mqtt.MqttClient((client) => {




		var sp = mqtt_host.split(":")
	   
        var random = Math.floor(Math.random() * sp.length);

        var host = sp[random];




        var socketOption = {
			rejectUnauthorized: false,
            host: host,

            port: mqtt_port						  
        };




        if (self.localIp != null) {

            socketOption.localAddress = self.localIp;

            socketOption.localPort = portIdx;

            portIdx++;

        }




        var ret = net.createConnection(socketOption, () => { });

        ret.on("error", (err) => {

            if (err.code != 'EADDRINUSE'&&err.code != 'ECONNREFUSED' && err.code != 'ETIMEDOUT' && err.code != 'ECONNRESET') {

				console.error("ret errror = " + err + "  " + self.responseTopic);
				portIdx--;

            }else{
				self.disconnect();
			}
			
			   
					// 소켓이 파괴된 경우에도 재연결 로직을 수행하지 않도록 추가
			//if (err.code == 'ERR_STREAM_DESTROYED') {
			//	self.disconnect();
			//}

        });
		ret.on("close", () => {
			// 소켓이 닫힌 경우에도 재연결 로직을 수행하도록 추가
			if (self.connected == true) {
				self.connected = false;
				if (numConn > 0) {
					numConn--;
				}
				this.disconnect();
			}
		});
	//	ret.on("destroy", () => {
    // 소켓이 파괴되었을 때 재연결 로직을 수행하도록 추가
	//	if (self.connected == true) {
	//		self.connected = false;
	//		if (numConn > 0) {
	//			numConn--;
	//		}
	//			this.disconnect();
	//		}
	//	})
        return ret;

        }, option);




        self.client.on("error", (err) => {

			console.error("err = " + err);

        if (self.connected == true) {

            self.connected = false;

            if (numConn > 0) {

				numConn--;

            }

            //self.disconnect()

        }

        });




        self.client.on("close", (err) => {

        if (self.connected == true) {

            self.connected = false;

            if (numConn > 0) {

           numConn--;

            }

            //self.disconnect();

        }

        });




        self.client.on('connect', function () {
			
            self.connected = true;

            self.isDead = false;

			


            if (self.isRecon == true) {

                self.isRecon = false;

               inReconnectDeviceCount--;

            }

            if (self.MAX_NUM >numConn) {

               numConn++;

            } else {

               numConn = self.MAX_NUM;

            }
			
			
        });




        self.client.subscribe(this.responseTopic);




        self.client.on("message", function (topic, message) {

           recvReqCount++;

            if (self.connected && self.client) {

                var parsed = JSON.parse(message.toString());

                if (parsed.cmd == "jsonRpc") {

                var res = {

                    cmd: "jsonRpc",

                    cmdId: parsed.cmdId,

                    serviceId: parsed.serviceId,

                    deviceId: parsed.deviceId,

                    result: "",

                    rpcRsp: {

                    jsonrpc: "2.0",

                    result: {

                        code: "000"

                    },

                    id: parsed.rpcReq.id

                    }

                };




                self.client.publish(self.requestTopic, JSON.stringify(res), { qos: 1 });

				sendReqCount++;

                }

            }
        });

    }

  }




  disconnect() {

    var self = this;

    self.connected = false;

    self.isDead = true;
	
    if (self.client) {

      self.client.end();

      self.client = null;

    }

    if (self.reconnectInterval > 0) {

      self.reconnect();

    }

  }




  reconnect() {
    var self = this;

    if (self.isRecon != true) {
      self.isRecon = true;

		inReconnectDeviceCount++;

    }

    setTimeout(() => {

      self.connect();

    }, Math.round(Math.random() * self.reconnectInterval));

  }




  sendTelemetry() {

    var self = this;

    if (self.connected == true && self.isDead == false) {

	
		if(self.client == null){
			self.connect();
		}
      if (self.connected == true && self.client) {

        var payload = getPayload();

        self.client.publish(self.telemetryTopic, JSON.stringify(payload), { qos: 1 });

		sendTelemetryCount++;

      }

    }
    
	if(rr == count){
		rr=0;
	}else{
		++rr;
	}
			  //Math.floor((self.rds * self.telemetryInterval));
	//var tel1= Math.floor((self.rds * self.telemetryInterval))
	setTimeout( () => {
		self.sendTelemetry()
	//	console.error("@ :"+self.randomNumbers[rr] )
	//	console.error("@@ :" + self.telemetryInterval)
	}, (self.randomNumbers[rr]/50.0)*(self.telemetryInterval))								   
  }


  run() {

    var self = this;
	if (self.connectAfter > 0) {

      //console.error("wait %s", self.connectAfter);

      setTimeout(() => {
		
        self.connect();
		
      }, self.connectAfter);

    } else {

      self.connect();

    }

    if (self.telemetryInterval > 0) {
	
					//Math.floor((randomNumbers[rcount]/100.0) * connectCompleteTime))/ 100.0;
     // var wait = Math.floor(((self.rds) * self.telemetryInterval))/1000.;
	 // console.error("wait !@ "+wait);
      setTimeout(() => {

        self.sendTelemetry();

      }, Math.random()*self.telemetryInterval);

    }

   

  }

}




module.exports = function (settings) {

  var services = settings.services;


  if (cluster.isMaster) {

    services.forEach((v, idx) => {

      var worker = cluster.fork();

      var localIp = null;

      if (settings.ip_addrs != null) {

        localIp = settings.ip_addrs[idx];

      }

      worker.send({

        settings: settings,

        service: v,

        localIp: localIp

      });

    });

  } else {

    process.on("message", (msg) => {

	  const randomNumbers = generateUniqueRandomNumbers(count, min, max);

      const startIdx = msg.settings.startIdx;

      const numDevice = msg.settings.numDevice;

      const telemetryInterval = msg.settings.telemetryInterval * 1000.0;

      const connectCompleteTime = msg.settings.connectCompleteTime * 1000;

      var reconnectInterval = 0;

      if (msg.settings.reconnectInterval != null) {

        reconnectInterval = msg.settings.reconnectInterval * 1000;

      }

      const json = require(msg.service);




      setInterval(() => {

        console.error(

          "%s : numConn = %s (%s), send telemetry/s = %s, recv req/s = %s, send req/s = %s, inRec = %s",

          json.serviceId,

         numConn,

          numDevice,

         sendTelemetryCount / 2,

         recvReqCount / 2,

         sendReqCount / 2,

         inReconnectDeviceCount

        );

       sendTelemetryCount = 0;

       sendReqCount = 0;

       recvReqCount = 0;
	
      }, 2000);




      for (var i = startIdx; i < startIdx + numDevice; i++) {

        var deviceId = json.serviceId + "Dev" + i;

        var deviceToken = json.serviceId + "Token" + i;
		//console.error("randomNumbers "+randomNumbers[rcount]/100);
        var rds = (randomNumbers[rcount])/100.0+0.0
		//console.error("@@@ : " + rds);
		var randomWait = (Math.floor(rds * connectCompleteTime));
		if(count == rcount){
			rcount=0;
		}else{
			++rcount;
		}
		


        var d = new Device(

          json.serviceId,

          deviceId,

          deviceToken,

          randomWait,

          telemetryInterval,

          msg.localIp,

          msg.settings.deadInterval,

          reconnectInterval,

          msg.settings.numDevice,
		  
		  rds

        );

        d.run();
		//console.error("randomNumbers "+randomWait);
      }

    });
  }

};




if (require.main == module) {

  if (process.argv.length < 3) {
    console.log("Usage: node %s settings.json", process.argv[1]);
    return;
  }

  var settings = require(process.argv[2]);
  module.exports(settings);

}