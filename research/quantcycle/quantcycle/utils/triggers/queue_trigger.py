from quantcycle.utils.get_logger import get_logger
import pika
import functools
import multiprocessing as mp
import sys
from datetime import datetime
import time
import queue
import os
from quantcycle.utils import setting as SETTING

get_logger.get_logger() = get_logger('tasks',log_path='tasks.log')

def on_message(chan, method_frame, properties, body, q):
    """Called when a message is received. Log message and ack it."""
    get_logger.get_logger().info(f'Received Message Body: {body}')
    chan.basic_ack(delivery_tag=method_frame.delivery_tag)
    q.put(body)
    sys.exit(0)
    

def consumer(q,parameters):
    # create a sub-process to consume rabbit mq message
    # received message will be then put in the queue:q
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    on_message_callback = functools.partial(on_message, q=q)
    channel.basic_consume(queue='BT150Status',
                          on_message_callback=on_message_callback,
                          auto_ack=True)
    channel.start_consuming()

def trigger(hour=0,minute=0,second=0,**kwargs):
    get_logger.get_logger().info(f'enter trigger at:{hour}:{minute}:{second}!')
    parameters = pika.URLParameters(SETTING.RABBITMQ_ADDR)
    time_out = hour * 10000 + minute * 100 + second
    
    q = mp.Queue()
    
    p = mp.Process(target=consumer,args=(q,parameters))
    p.start()
    
    while(True):
        now = datetime.now()
        get_logger.get_logger().info(f'now:{now}')
        time_now = now.hour * 10000 + now.minute * 100 + now.second
        res = None
        if time_now >= time_out:
            get_logger.get_logger().info(f'Timeout!')
            p.terminate()
            return False
        try:
            res = q.get_nowait()
            get_logger.get_logger().info(f'bt150 rabbitmq pulished:{res}')
        except queue.Empty:
            pass
        else:
            p.terminate()
            is_success = str(res).split(' ')[0] == "success"
            get_logger.get_logger().info(f'bt150 status:{is_success}')
            return is_success
        time.sleep(1)
    return
