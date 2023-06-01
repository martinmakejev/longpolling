import http from 'http';
import url from 'url';
import process from 'process';
import fetch from 'node-fetch';

const port = process.env.PORT || 4000;
const subscriptionKey = process.env.SUBSCRIPTION_KEY || "test-subscription-key";
const publishKey = process.env.PUBLISH_KEY || "test-publish-key";
const PLC_GetDataKey = process.env.PLC_GET_DATA_KEY || "";
const PLC_GetSetDataKey = process.env.PLC_GET_SET_DATA_KEY || "";
const getPLCGetDataEndpoint = (deviceId) => `${process.env.PLC_BASE_URL}/api/PLC_GetData/${deviceId}?code=${PLC_GetDataKey}`
const getPLCGetSetDataEndpoint = (deviceId) => `${process.env.PLC_BASE_URL}/api/PLC_GetSetData/${deviceId}?code=${PLC_GetSetDataKey}`
const subscriberTimeout = process.env.SUBSCRIBER_TIMEOUT || 600000; // 10 minutes

/**
 * Object key is device ID, value is array of subscribers
 * @type {Object.<string, http.ServerResponse[]>}
 * @example
 * {
 *  "gen2-hansa": [Response, Response],
 *  "gen2-ikea": [Response]
 * }
 */
let subscribers = {};

/**
 * Main entry point for the server
 * @param {http.IncomingMessage} req
 * @param {http.ServerResponse} res
 * @returns {void}
 */
function handleRequest(req, res) {

    // Debug messages
    req.on('end', () => {
        console.log('Incoming connection end');
    });
    res.on('end', () => {
        console.log('Outgoing connection end');
    });
    res.on('finish', () => {
        console.log('Outgoing connection finish');
    });
    res.on('close', () => {
        console.log('Outgoing connection closed');
    });
    res.on('error', (e) => {
        console.error(`Outgoing connection error: ${e.message}`);
    });

    // Global error handler to avoid crashing the server
    try {
        // Parse URL
        const parsedUrl = url.parse(req.url, true);

        if (parsedUrl.query.ping) {
            res.end(`{"ping": "success"}`);
            return;
        }

        let deviceId = parsedUrl.query.device || null;
        let code = parsedUrl.query.code;

        if (req.method === "OPTIONS") {
            res.setHeader('Access-Control-Allow-Origin', '*');
            res.end();
        }

        if (deviceId === null) {
            res.statusCode = 400;
            res.end(`{"error": "Missing 'device' query parameter"}`);
            return;
        }

        if (parsedUrl.pathname === '/subscribe') {
            if (code !== subscriptionKey) {
                res.statusCode = 403;
                res.end(`{"error": "Invalid subscription key"}`);
                return;
            }
            handleSubscribeRequest(req, res, deviceId);
        }

        if (urlParsed.pathname === '/publish' && req.method === 'POST') {
            if (code !== publishKey) {
                res.statusCode = 400;
                res.end(`{"error": "Invalid code"}`);
                return;
            }
            handlePublishRequest(req, res, deviceId);
        }
    }
    catch (e) {
        console.error(e);
        res.statusCode = 500;
        res.end();
    }
}

function handleSubscribeRequest(req, res, deviceId) {
    // Accept data sent by device, pass it to Azure Function "PLC_GetSetData"
    if (req.method === "POST") {
        console.log('POST /subscribe accepting data');
        req.setEncoding('utf8');
        let message = '';
        req.on('data', function (chunk) {
            message += chunk;
        }).on('end', function () {
            console.log('POST /subscribe end of data', message);
            // If body has empty data, do not send it to Azure Function
            // e.g. data === "{}" || data === "[]" || data === ""
            if (message.length <= 2)
                GetDeviceData(deviceId).then(parseDatabaseResponse.bind(null, req, res, deviceId));
            else
                GetSetDeviceData(deviceId, message).then(parseDatabaseResponse.bind(null, req, res, deviceId));
        });
        return;
    }

    // Get latest data for device from Azure Function "PLC_GetData"
    if (req.method === "GET") {
        console.log('GET /subscribe');
        GetDeviceData(deviceId).then(parseFunctionResponse);
        return;
    }
}

function handlePublishRequest(req, res) {
    console.log("Got signal for new data");
    const subscriberCount = Object.keys(subscribers[deviceId] || {}).length
    if (!subscriberCount) {
        console.log(`There are 0 listeners for ${deviceId}, don't fetch data as there is nobody to send it to`);
    } else {
        console.log(`There are ${subscriberCount} listeners for ${deviceId}, fetch new data from Azure`);
        GetDeviceData(deviceId).then(data => {
            sendDataToSubscribers(deviceId, data);
        }).catch(err => {
            console.log("GetDeviceData catch", err);
        });
    }
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.statusCode = 204;
    res.end();
}

function saveSubscriber(req, res, deviceId) {
    console.log(`Subscribing ${deviceId}`);

    // Check if any device is already subscribed
    if (!subscribers[deviceId]) {
        subscribers[deviceId] = [];
    }
    subscribers[deviceId].push(res);

    // Remove subscriber when connection is closed
    req.on('close', function () {
        console.log(`Subscriber ${deviceId} closed connection, removing from listeners`);
        subscribers[deviceId] = subscribers[deviceId].filter((subscriber) => subscriber !== res);
    });

    // Force close connection after 10 minutes
    req.socket.setTimeout(subscriberTimeout, () => {
        console.log(`Subscriber ${deviceId} connection timeout`);
        res.end();
    });
}

function GetDeviceData(deviceId) {
    console.log("GetDeviceData", deviceId);
    return fetch(getPLCGetDataEndpoint(deviceId), { method: "POST" }).then(res => res.text());
}

function GetSetDeviceData(deviceId, body) {
    console.log("GetSetDeviceData", deviceId);
    return fetch(getPLCGetSetDataEndpoint(deviceId), { method: "POST", body }).then(res => res.text());
}

/**
 * Try to get latest data for device. If there is no new data subscribe for updates.
 * If there is new data, send it back immediately - no need for subscription. 
 */
function parseDatabaseResponse(req, res, deviceId, data) {
    console.log('Azure had following data for us: ', data);
    // length<=2 should detect empty body, empty object and empty array
    // e.g. data === "{}" || data === "[]" || data === ""
    if (data.length <= 2) {
        console.log('Azure data is empty, we keep connection open until new data signal arrives');
        saveSubscriber(req, res, deviceId);
    } else {
        console.log('Got data from Azure, no subscription needed, sending data now');
        sendDataToSubscribers(deviceId, data);
        res.end(data);
    }
}

function sendDataToSubscribers(deviceId, data) {
    console.log(`Publishing ${deviceId} data: ${data}`);
    if (subscribers[deviceId]) {
        subscribers[deviceId].forEach((res) => {
            // Send data to subscriber and close connection
            // We don't want to keep the connection open as connections may drop and it keeps code in IoT device simple
            res.end(data);
        });
    }
    subscribers[deviceId] = [];
}

function gracefullyShutdown() {
    console.log("Server is shutting down, close all connections")
    for (let deviceId in subscribers) {
        if (subscribers.hasOwnProperty(deviceId)) {
            subscribers[deviceId].forEach((res) => {
                res.end();
            });
        }
    }
    setTimeout(process.exit, 1000);
}

http.createServer(handleRequest).listen(port);
console.log(`Server running on port ${port}`);
if (subscribeKey === "test-subscription-key") {
    console.warn(`Warning: SUBSCRIBE_KEY enviroment variable is not set!`);
}
if (publishKey === "test-publish-key") {
    console.warn(`Warning: PUBLISH_KEY enviroment variable is not set!`);
}
if (PLC_GetDataKey === "") {
    console.warn(`Warning: PLC_GET_DATA_KEY enviroment variable is not set!`);
}
if (PLC_GetSetDataKey === "") {
    console.warn(`Warning: PLC_GET_SET_DATA_KEY enviroment variable is not set!`);
}

process.on('SIGINT', gracefullyShutdown);
process.on('SIGTERM', gracefullyShutdown);
process.on('beforeExit', gracefullyShutdown);