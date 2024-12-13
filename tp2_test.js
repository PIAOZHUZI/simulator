const cluster = require("cluster");
const mqtt = require("mqtt");
const net = require("net");

const mqtt_host = process.env.THINGPLUG_HOST1 || 'localhost';
const http_host = process.env.THINGPLUG_HTTP_HOST || mqtt_host;

let numConn = 0;
let sendTelemetryCount = 0;
let recvReqCount = 0;
let sendReqCount = 0;
let inReconnectDeviceCount = 0;

const http_port = 9000;
const mqtt_port = 1883;
let portIdx = 10000;

function getPayload() {
    return { "temperature": 10 + Math.round(Math.random() * 20) };
}

class Device {

    constructor(serviceId, deviceId, deviceToken, connectAfter, telemetryInterval, localIp, reconnectInterval) {
        this.requestTopic = `v1/dev/${serviceId}/${deviceId}/up`;
        this.responseTopic = `v1/dev/${serviceId}/${deviceId}/down`;
        this.telemetryTopic = `v1/dev/${serviceId}/${deviceId}/telemetry`;
        this.deviceToken = deviceToken;
        this.telemetryInterval = telemetryInterval;
        this.connectAfter = connectAfter;
        this.localIp = localIp;
        this.connected = false;
        this.isRecon = false;
        this.reconnectInterval = reconnectInterval;
        this.client = null;
    }

    connect() {
        if (this.connected) return;
        var sp = mqtt_host.split(":")
        var random = Math.floor(Math.random() * sp.length);
        var host = sp[random];

        const options = {
            host: host,
            port: mqtt_port,
            clientId: this.deviceToken,
            username: this.deviceToken,
            clean: true,
            keepalive: 120,
        };

        try {
            this.client = mqtt.connect(options);

            this.client.on("connect", () => {
                this.connected = true;
                if (this.isRecon) {
                    this.isRecon = false;
                    inReconnectDeviceCount = Math.max(inReconnectDeviceCount - 1, 0); // 동기화 보장
                }
                numConn++;
            });

            this.client.on("message", (topic, message) => {
                recvReqCount++;
                const parsed = JSON.parse(message.toString());
                if (parsed.cmd === "jsonRpc") {
                    const res = {
                        cmd: "jsonRpc",
                        cmdId: parsed.cmdId,
                        serviceId: parsed.serviceId,
                        deviceId: parsed.deviceId,
                        result: "",
                        rpcRsp: { jsonrpc: "2.0", result: { code: "000" }, id: parsed.rpcReq.id }
                    };
                    this.client.publish(this.requestTopic, JSON.stringify(res), { qos: 1 });
                    sendReqCount++;
                }
            });

            this.client.on("error", (err) => {
                console.error(`MQTT error: ${err.message}`);
                this.disconnect();
            });

            this.client.on("close", () => {
                if (this.connected) {
                    this.connected = false;
                    numConn--;
                    this.reconnect();
                }
            });

            this.client.subscribe(this.responseTopic);
        } catch (err) {
            console.error(`Failed to connect: ${err.message}`);
            this.reconnect();
        }
    }

    disconnect() {
        if (this.client) {
            this.client.end();
            this.client = null;
        }
        this.connected = false;
        if (this.reconnectInterval > 0) {
            this.reconnect();
        }
    }

    reconnect() {
        if (!this.isRecon) {
            this.isRecon = true;
            inReconnectDeviceCount++;
        }
        setTimeout(() => this.connect(), this.reconnectInterval + Math.random() * 1000);
    }

    sendTelemetry() {
        if (!this.connected) return;
        const payload = getPayload();
        this.client.publish(this.telemetryTopic, JSON.stringify(payload), { qos: 0 });
        sendTelemetryCount++;
        const randomDelay = (this.deviceToken.hashCode() % this.telemetryInterval) + (Math.random() * 100);

        setTimeout(() => this.sendTelemetry(), randomDelay);
    }

    run() {
        setTimeout(() => this.connect(), this.connectAfter);
        const randomDelay = (this.deviceToken.hashCode() % this.telemetryInterval) + (Math.random() * 100);

        setTimeout(() => this.sendTelemetry(), randomDelay);
    }
}
String.prototype.hashCode = function () {
    let hash = 0;
    for (let i = 0; i < this.length; i++) {
        const char = this.charCodeAt(i);
        hash = (hash << 5) - hash + char;
        hash |= 0; // Convert to 32-bit integer
    }
    return Math.abs(hash); // Always return positive value
};

module.exports = function (settings) {
    const services = settings.services;

    if (cluster.isMaster) {
        services.forEach((v, idx) => {
            const worker = cluster.fork();
            worker.send({ settings, service: v, localIp: settings.ip_addrs ? settings.ip_addrs[idx] : null });
        });
    } else {
        process.on("message", (msg) => {
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
                    `${json.serviceId} : numConn/${numDevice} = ${numConn}, send telemetry/s = ${sendTelemetryCount / 2}, recv req/s = ${recvReqCount / 2}, send req/s = ${sendReqCount / 2}, inRec = ${inReconnectDeviceCount}`
                );
                sendTelemetryCount = recvReqCount = sendReqCount = 0;
            }, 2000);

            for (let i = startIdx; i < startIdx + numDevice; i++) {
                const deviceId = `${json.serviceId}Dev${i}`;
                const deviceToken = `${json.serviceId}Token${i}`;
                const connectAfter = (connectCompleteTime / numDevice) * (i - startIdx) + Math.random() * 500;
                const device = new Device(
                    json.serviceId,
                    deviceId,
                    deviceToken,
                    connectAfter,
                    telemetryInterval,
                    msg.localIp,
                    reconnectInterval
                );
                device.run();
            }
        });
    }
};

if (require.main === module) {
    if (process.argv.length < 3) {
        console.error(`Usage: node ${process.argv[1]} settings.json`);
        process.exit(1);
    }
    const settings = require(process.argv[2]);
    module.exports(settings);
}
