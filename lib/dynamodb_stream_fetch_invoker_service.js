'use strict';

let constants     = require('./../config/constants.js');

const async        = require('async')
const EventEmitter = require('events')
const _            = require('lodash');
const childProcess = require('child_process');
const path         = require('path');
const Redis        = require('./redis.js');
const PartitionFetcher = require('./partitions_fetcher.js');

let internals = {};
internals.streamProcesses = [];

internals.allStreamsLaunched = function allStreamsLaunched(streamConfig){
    let totalStreamsRequired = 0;

    _.map(streamConfig.streams, function eachStream(stream) {
        totalStreamsRequired += (1 * stream.partitions);
    })

    return function(){

        if (internals.streamProcesses.length == totalStreamsRequired) {
            return true;
        }else{
            return false;
        }
    }
}

internals.streamConfigMissingError = function streamConfigMissingError(){
    throw new Error('StreamConfig must be an Object and cannot be empty. \n ' +
        '            Expected Parameters are: \n' +
        '            { redisUrl: _redisUrl_\n' +
        '              streams: [\n' +
        '                   name: _some_stream_name_,\n' +
        '                   partitions: _partitions_for_stream_\n' +
                        '] \n' +
        '              }');


};

internals.redisUrlMissingError = function redisUrlMissingError(){
    throw new Error('redisUrl is invalid');
}

class DynamodbStreamFetchInvoker extends EventEmitter{

    constructor(streamConfig){
        super();

        if (!(_.isPlainObject(streamConfig) || _.isEmpty(streamConfig))) {
            return internals.streamConfigMissingError();
        }

        this.streamConfig = streamConfig;

        if(_.isEmpty(_.trim(streamConfig.redisUrl)))
            return internals.redisUrlMissingError();

        this.redisClient  = new Redis(streamConfig.redisUrl);
    };

    streamProcesses(){
        return internals.streamProcesses
    };

    generateKey(streamArn, shardId){
        return (this.streamConfig.instanceId + ":" + streamArn.toString() + "-" + shardId.toString());
    };

    setupExitHandlers(child, streamArn){

        let self = this;
        child.on('exit', function onExit(){

            let processObject = _.find(internals.streamProcesses, (streamObject)  => {
                return  (streamObject.streamArn == streamArn)
            });

            _.pull(internals.streamProcesses, processObject);

            function onLastReadState(err, streamArn, shardId, lastReadSequenceNumber){
                if (err) {
                    throw err;
                }else{
                    self.launchStreamFetcher(streamArn, 1, shardId, lastReadSequenceNumber)
                }

            }
            self.findLastReadState(streamArn, processObject.shardId, onLastReadState)
        })
    }

    setupMessageListeners(child){
        let self = this;
        child.on('message', function onMessage(data){

            self.saveLastReadState(data.payload.streamArn, data.payload.shardId, data.payload.lastSequenceNumber, (err, reply) => {

                if(err) {
                    throw err;
                }else{
                    console.log(reply);
                }
            });
            self.emit('message', data);
        });
    };

    saveLastReadState(streamArn, shardId, lastReadSequenceNumber, cb){

        let key    = this.generateKey(streamArn, shardId);
        let value  = JSON.stringify({shardId: shardId, lastReadSequenceNumber: lastReadSequenceNumber});
        this.redisClient.set(key, value, cb);
    };

    findLastReadState(streamArn, shardId, cb){

        let key = this.generateKey(streamArn, shardId);
        this.redisClient.get(key, function onGet(err, reply){

            if(err){
                return cb(err, null);
            }else{
                if (reply == null) {
                    return cb(null, streamArn, null, null);
                }else{
                    let response               = JSON.parse(reply);
                    let lastReadSequenceNumber = response.lastReadSequenceNumber;
                    return cb(null, streamArn, shardId, lastReadSequenceNumber);
                }
            }
        });
    };

    launchStreamFetcher(streamArn, partitions, shardId, lastReadSequenceNumber){
        console.log('Called with', streamArn, shardId, lastReadSequenceNumber);

        let filePath              = (path.resolve(__dirname) + '/fetcher_service.js');
        let child                 = childProcess.fork(filePath);
        let processObject         = {
            streamArn: streamArn,
            streamProcess: child,
            partitions: partitions,
            shardId: shardId,
            lastReadSequenceNumber: lastReadSequenceNumber
        };

        internals.streamProcesses.push(processObject);
        console.log('Child Processes Count', internals.streamProcesses.length);
        this.setupExitHandlers(child, streamArn);

        let payload = {
            streamArn: processObject.streamArn,
            partitions: processObject.partitions
        };

        _.merge(payload, {shardId: shardId, lastReadSequenceNumber: lastReadSequenceNumber})

        child.send({
            command: constants.init,
            payload: payload
        });

        this.setupMessageListeners(child);
    };

    getPartitionInfoAndLaunchStreamFetcher(streamArn){

        let self              = this;
        let partitionsFetcher = new PartitionFetcher(streamArn);
        partitionsFetcher.init(function onPartitionsData(data){

            let payload = data.payload;
            _.map(payload.shardIdsAndSequenceNumbers, function onEachShardIdAndSequenceNumber(shardIdAndSequenceNumber){

                let startingSequenceNumber = shardIdAndSequenceNumber[1];
                let shardId                = shardIdAndSequenceNumber[0];
                self.findLastReadStateAndLaunch(streamArn, shardId, startingSequenceNumber)
            })
        })
    };


    findLastReadStateAndLaunch(streamArn, shardId, startingSequenceNumber){

        let self = this;
        this.findLastReadState(streamArn, shardId, function onLastReadState(err, _streamArn, _shardId, lastReadSequenceNumber){

            if(err || (lastReadSequenceNumber == null)){
                self.launchStreamFetcher(streamArn, 1, shardId, startingSequenceNumber);
            }else{
                self.launchStreamFetcher(streamArn, 1, shardId, lastReadSequenceNumber);
            }
        })
    };

    fetch(done) {

        let self = this;
        try{
            _.map(this.streamConfig.streams, function eachStream(stream){

                self.getPartitionInfoAndLaunchStreamFetcher(stream.name);
            })

        }catch(err){
            console.log(err.stack)
        }
    }
}


process.on('SIGINT', () => {
    console.log("Stopping child processes....");
    _.map(internals.streamProcesses, (processObject) => {

       processObject.streamProcess.removeAllListeners();
       processObject.streamProcess.kill();
    });
    console.log("Done.");

    process.exit(0);
})


module.exports = DynamodbStreamFetchInvoker;