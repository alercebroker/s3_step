##################################################
#       s3_step   Settings File
##################################################
import os


CONSUMER_CONFIG = {
    "PARAMS": {
        "bootstrap.servers": os.environ["CONSUMER_SERVER"],
        "group.id": os.environ["CONSUMER_GROUP_ID"],
        "auto.offset.reset": "beginning",
        "enable.partition.eof": os.getenv("ENABLE_PARTITION_EOF", False),
        'max.poll.interval.ms' : 3600000
    },
}

if os.getenv("TOPIC_STRATEGY_FORMAT"):
    CONSUMER_CONFIG["TOPIC_STRATEGY"] = {
        "CLASS": "apf.core.topic_management.DailyTopicStrategy",
        "PARAMS": {
            "topic_format": os.environ["TOPIC_STRATEGY_FORMAT"].strip().split(","),
            "date_format": "%Y%m%d",
            "change_hour": 23,
        },
    }
elif os.getenv("CONSUMER_TOPICS"):
    CONSUMER_CONFIG["TOPICS"] = os.environ["CONSUMER_TOPICS"].strip().split(",")
else:
    raise Exception("Add TOPIC_STRATEGY or CONSUMER_TOPICS")


STORAGE_CONFIG = {
    "BUCKET_NAME": os.environ["BUCKET_NAME"],
    "REGION_NAME": os.environ["REGION_NAME"],
}

LOGGING_DEBUG = os.getenv("LOGGING_DEBUG", False)

def generate_key(message):

    # Get the last source of an object by max mjd value 
    last_source_id = max(message['sources'], key=lambda x: x['mjd'])['sourceid']
    return "{}_{}".format(message['objectid'], last_source_id)

STEP_CONFIG = {
    "STORAGE": STORAGE_CONFIG,
    "CONSUMER_CONFIG": CONSUMER_CONFIG,
    "KEY": generate_key
}
