var util = require('util');
var EventEmitter = require('events').EventEmitter;
var thrift = require('./thrift');

var TBufferedTransport = require('./buffered_transport');
var TBinaryProtocol = require('./binary_protocol');
var InputBufferUnderrunError = require('./input_buffer_underrun_error');

var LambdaConnection = exports.LambdaConnection = function(lambda, functionName, options) {
  EventEmitter.call(this);

  var self = this;
  this.options = options || {};
  this.lambda = lambda;
  this.functionName = functionName;
  this.transport = this.options.transport || TBufferedTransport;
  this.protocol = this.options.protocol || TBinaryProtocol;

  this.seqId2Service = {};

  function decodeCallback(transport_with_data) {
    var proto = new self.protocol(transport_with_data);
    try {
      while (true) {
        var header = proto.readMessageBegin();
        var dummy_seqid = header.rseqid * -1;
        var client = self.client;
        var service_name = self.seqId2Service[header.rseqid];
        if (service_name) {
          client = self.client[service_name];
          delete self.seqId2Service[header.rseqid];
        }
        client._reqs[dummy_seqid] = function(err, success){
          transport_with_data.commitPosition();
          var clientCallback = client._reqs[header.rseqid];
          delete client._reqs[header.rseqid];
          if (clientCallback) {
            process.nextTick(function() {
              clientCallback(err, success);
            });
          }
        };
        if(client['recv_' + header.fname]) {
          client['recv_' + header.fname](proto, header.mtype, dummy_seqid);
        } else {
          delete client._reqs[dummy_seqid];
          self.emit("error",
                    new thrift.TApplicationException(
                       thrift.TApplicationExceptionType.WRONG_METHOD_NAME,
                       "Received a response to an unknown RPC function"));
        }
      }
    }
    catch (e) {
      if (e instanceof InputBufferUnderrunError) {
        transport_with_data.rollbackPosition();
      } else {
        self.emit('error', e);
      }
    }
  }

  this.write = function(data, seqid) {
    var self = this;
    self.lambda.invoke({
      FunctionName: self.functionName,
      Payload: JSON.stringify({data: data.toString('base64')})
    }).promise()
    .then(function (data) {
      var buf = Buffer.from(data.Payload, 'base64');
      self.transport.receiver(decodeCallback)(buf);
    }, function (err) {
      var service_name = self.seqId2Service[seqid];
      if (service_name) {
        delete self.seqId2Service[seqid];
        var client = self.client[service_name];
        var clientCallback = client._reqs[seqid];
        delete client._reqs[seqid];
        if (clientCallback) {
          process.nextTick(function() {
            clientCallback(err);
          });
          return;
        }
      }
      self.emit("error", err);
    });
  };
};
util.inherits(LambdaConnection, EventEmitter);

exports.createLambdaConnection = function(lambda, functionName, options) {
  return new LambdaConnection(lambda, functionName, options);
};
