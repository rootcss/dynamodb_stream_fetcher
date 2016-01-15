'use strict';

let Aws              = require('./aws');
let constants        = require('./../config/constants.js');

const _              = require('lodash');
const async          = require('async')
const Dynamo         = new Aws.DynamoDBStreams({apiVersion: '2012-08-10'});
const messageBuilder = require('./message_builder')

let getShardsAndSequenceNumbers = function getShardsAndSequenceNumbers(payload, cb){

    let streamArn = payload.streamArn;
    getShardIds(streamArn, function onShardInfo(err, shardIds, startingSequenceNumbers){

        let zippedShardIdAndSequenceNumber = _.zip(shardIds, startingSequenceNumbers);
        let getShardIdsPayload             = {streamArn: streamArn,
                                              shardIdsAndSequenceNumbers: zippedShardIdAndSequenceNumber};
        let message                        = messageBuilder.build(constants.shardInfoResponse, getShardIdsPayload);
        return cb(message);
    })
}

let getShardIds = (streamArn, cb) => {

    let describeStreamParams = {
        StreamArn: streamArn,
        Limit: 10
    };

    Dynamo.describeStream(describeStreamParams, (err, data) => {

        if(err) { return cb(err, null); }
        else {
            let shardIds                = _.collect(data.StreamDescription.Shards, (shardPayload) => { return shardPayload.ShardId} );
            let startingSequenceNumbers = _.collect(data.StreamDescription.Shards, (shardPayload) => {return shardPayload.SequenceNumberRange.StartingSequenceNumber})
            return cb(null, shardIds, startingSequenceNumbers);
        }

    })
};

let getShardIterator = (streamArn, shardId, startingSequenceNumber, cb) => {

    let shardIteratorType = 'AT_SEQUENCE_NUMBER';

    let shardIteratorParams = {
        ShardId: shardId,
        ShardIteratorType: shardIteratorType, /* required */
        StreamArn: streamArn,
        SequenceNumber: startingSequenceNumber
    };
    Dynamo.getShardIterator(shardIteratorParams, (err, data) => {

        if(err) { return cb(err, null); }
        else{ return cb(null, data.ShardIterator); }
    })
};

let getRecords = (shardIterator, cb) => {

    let getRecordParams = {
        ShardIterator: shardIterator,
        Limit: 5
    };
    Dynamo.getRecords(getRecordParams, (err, data) => {

        if(err){ return cb(err, null); }
        else{ return cb(null, data); }
    })
}

let sendDataToParent = (records, streamArn, shardId, lastReadSequenceNumber) => {

    if (records.length > 0){
        let message = {
            dataCount: records.length,
            lastSequenceNumber: lastReadSequenceNumber,
            records: records,
            streamArn: streamArn,
            shardId: shardId,

        };

        process.send({
            command: constants.data,
            payload: message
        });
    }
};


class FetcherService{
    constructor(streamArn, partitions, shardId, lastReadSequenceNumber){
        this.streamArn = streamArn;
        this.partitions = partitions;
        this.shardId    = shardId;
        this.lastReadSequenceNumber = lastReadSequenceNumber;
    };

    init(){
        let self = this;

        if ((this.shardId) && (this.lastReadSequenceNumber)){
            getShardIterator(this.streamArn, this.shardId, this.lastReadSequenceNumber, (err, shardIterator) => {

                console.log("Fetching From last fetched point");
                if(err) { throw err; }
                else{ this.loopAndGetRecords(shardIterator); }

            })

        }else{
            this.fetchFromBeginningOfStream((err, shardIterator) => {


                console.log("Fetching from the beginning");
                if(err) { throw err; }
                else{ this.loopAndGetRecords(shardIterator); }
            })
        }
    };

    loopAndGetRecords(shardIterator){
        let self     = this;
        let iterator = shardIterator;

        async.forever((next) => {

            getRecords(iterator, (err, data) => {

                if(err){
                    console.log(err);
                    throw err;
                }
                else{
                    iterator                    = data.NextShardIterator;
                    let records                 = data.Records;
                    self.lastReadSequenceNumber = ((records.length > 0) ? records[records.length - 1].dynamodb.SequenceNumber : null);
                    sendDataToParent(records, this.streamArn, this.shardId, this.lastReadSequenceNumber);

                    setTimeout(function(){
                        next();
                    }, 1000);

                }
            })
        })
    };

    fetchFromBeginningOfStream(cb){
        let self = this;
        let streamArn = this.streamArn;

        async.waterfall([
            function(callback){

                getShardIds(streamArn, (err, shardIds, startingSequenceNumbers) => {

                    return callback(err, shardIds, startingSequenceNumbers)
                })
            },
            function(shardIds, startingSequenceNumbers, callback){

                self.shardId = shardIds[0];
                getShardIterator(streamArn, shardIds[0], startingSequenceNumbers[0], (err, data) => {

                   return callback(err, data)
                })
            }
        ], (err, shardIterator) => {

            if(err){ return cb(err, null); }
            else{ return cb(null, shardIterator); }

        })

    };
}

process.on('message', function onMessage(data){

    if (data.command == constants.init) {
        console.log('Launching...')
        let fetcherService = new FetcherService(data.payload.streamArn,
                                                data.payload.partitions,
                                                data.payload.shardId,
                                                data.payload.lastReadSequenceNumber);
        fetcherService.init();
    } else if (data.command == constants.shardInfo) {
        getShardsAndSequenceNumbers(data.payload, function onShardsAndSequenceNumbers(message) {
            process.send(message);
        });
    }else {
        console.log('Invalid Command');
        process.exit(0);
    }

})

module.exports = FetcherService;