from kafka import KafkaConsumer
from ddtrace import tracer
from ddtrace import patch_all
from ddtrace.context import Context
import opentracing
from ddtrace.opentracer import Tracer, set_global_tracer
import time
from opentracing.propagation import Format
from ddtrace.opentracer.span_context import SpanContext
# from opentracing.tracer import Tracer
# from ddtrace.opentracer import Tracer


class MyRPCPropagator(object):
    def inject(self, span_context, rpc_metadata):
        rpc_metadata.update({
            'trace_id': span_context.trace_id,
            'span_id': span_context.span_id,
        })

    def extract(self, rpc_metadata):
        # return SpanContext(
        return Context(
            trace_id=rpc_metadata['trace_id'],
            span_id=rpc_metadata['span_id'],
            sampling_priority=rpc_metadata['sampling_priority']
        )


def init_tracer(service_name):
    config = {
        "agent_hostname": "localhost",
        "agent_port": 8126,
    }
    tracer = Tracer(service_name, config=config)
    set_global_tracer(tracer)
    return tracer

@tracer.wrap(service="kafka_consumer")
def sayHello(method, rpc_metadata):
    # tracer = init_tracer("kafka-consumer")
    propagator = MyRPCPropagator()
    context = propagator.extract(rpc_metadata)
    # context = tracer.extract(format=Format.TEXT_MAP,carrier=rpc_metadata)
    tracer.context_provider.activate(context)
    with tracer.trace(service="kafka_consumer",name="kafka_consumer_span") as span:
    # with tracer.active_span() as span:
    # with opentracing.tracer.start_span('kafka_consumer', child_of=context) as span:
        span.set_meta("my_rpc_method", method)
        time.sleep(0.03)
        print("Hi, Hello")
        span.finish()
    # tracer.finish()
    # tracer.close()
    # with tracer.trace("child_span") as span:
    #     span.set_meta("my_kafka_consumer","sayHello")
    # span = opentracing.tracer.start_span("kafkaconsumer")
    # span.set_tag("tagkey", "tagvalue")
    # time.sleep(0.05)
def kafukaconsume():
    # consumer = KafkaConsumer('topic-name',#  group_id='my-group',
    #                         bootstrap_servers=['localhost:9092'])
    consumer = KafkaConsumer(
        'topic-name', bootstrap_servers=['localhost:9092'])
    # init_tracer("kafka-consumer")
    # span=opentracing.tracer.start_span("kafkaconsumer")
    # span.set_tag("tagkey","tagvalue")
    # time.sleep(0.05)
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic,
              message.partition, message.offset, message.key, message.value))
        tid = str(message.key, encoding='utf-8').split(",")[0]
        sid = str(message.key, encoding='utf-8').split(",")[1]
        # print ("tid is : %s, sid is : %s" % (tid,sid))
        rpc_metadata = {}
        rpc_metadata["trace_id"] = int(tid)
        rpc_metadata["span_id"] = int(sid)
        rpc_metadata["sampling_priority"] = 1
        # do somthing like sayHello according to the message
        sayHello("kafka_consumer", rpc_metadata)
        # span.finish()


def main():
    # To consume latest messages and auto-commit offsets

    kafukaconsume()
    # sayHello()


if __name__ == "__main__":
    main()


# # consume earliest available messages, don't commit offsets
# KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# # consume json messages
# KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

# # consume msgpack
# KafkaConsumer(value_deserializer=msgpack.unpackb)

# # StopIteration if no message after 1sec
# KafkaConsumer(consumer_timeout_ms=1000)

# # Subscribe to a regex topic pattern
# consumer = KafkaConsumer()
# consumer.subscribe(pattern='^awesome.*')

# # Use multiple consumers in parallel w/ 0.9 kafka brokers
# # typically you would run each on a different server / process / CPU
# consumer1 = KafkaConsumer('my-topic',
#                           group_id='my-group',
#                           bootstrap_servers='my.server.com')
# consumer2 = KafkaConsumer('my-topic',
#                           group_id='my-group',
#                           bootstrap_servers='my.server.com')
