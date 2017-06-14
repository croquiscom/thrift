var MultiplexedProcessor = require('./multiplexed_processor').MultiplexedProcessor;

var TBufferedTransport = require('./buffered_transport');
var TBinaryProtocol = require('./binary_protocol');
var InputBufferUnderrunError = require('./input_buffer_underrun_error');

exports.createLambdaHandler = function(options) {
  var svcObj = options.service;
  if (svcObj.processor instanceof MultiplexedProcessor) {
    svcObj.processor = svcObj.processor;
  } else {
    var processor = (svcObj.processor) ? (svcObj.processor.Processor || svcObj.processor) :
                                         (svcObj.cls.Processor || svcObj.cls);
    if (svcObj.handler) {
      svcObj.processor = new processor(svcObj.handler);
    } else {
      svcObj.processor = processor;
    }
  }
  svcObj.transport = svcObj.transport ? svcObj.transport : TBufferedTransport;
  svcObj.protocol = svcObj.protocol ? svcObj.protocol : TBinaryProtocol;

  return function (event, context, callback) {
    var svc = svcObj;

    var receiver = svc.transport.receiver(function(transportWithData) {
      var input = new svc.protocol(transportWithData);
      var output = new svc.protocol(new svc.transport(undefined, function(buf) {
        callback(null, buf.toString('base64'));
      }));

      try {
        svc.processor.process(input, output);
        transportWithData.commitPosition();
      } catch (err) {
        if (err instanceof InputBufferUnderrunError) {
          transportWithData.rollbackPosition();
        } else {
          callback(new Error('unknown error'));
        }
      }
    });
    receiver(Buffer.from(event.data, 'base64'));
  }
};
