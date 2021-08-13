#!/usr/bin/env python

import tweepy
import logging
from config import create_api
import json
import boto3
import FirehoseManager as fh_s3

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(asctime)s: %(message)s');
logger = logging.getLogger();

class TweetListener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        self.me = api.me()
        self.firehose_client = boto3.client('firehose');
        self.firehose_name = 'covid19_fh_stream';
        self.bucket_arn = 'arn:aws:s3:::covid19-fh-stream';
        self.iam_role_name = 'KinesisFHToS3-ServiceRole';

        firehose_arn = None;
        
        if not fh_s3.firehose_exists(self.firehose_name):
            firehose_arn = fh_s3.create_firehose_to_s3(self.firehose_name, self.bucket_arn, self.iam_role_name);
            if firehose_arn is None:
                logging.info('Firehose does not exist');
                exit(1)

        logging.info(f'Created Firehose delivery stream to S3: {firehose_arn}')

        # Wait for the Firehose to become active
        if not fh_s3.wait_for_active_firehose(self.firehose_name):
            exit(1)
        logging.info('Firehose stream is active')

    def on_status(self, tweet):
        toprint = str(tweet.id) + ': '
        toprint += tweet.user.screen_name + "@"

        if not(tweet.user.location is None):
            toprint += tweet.user.location
        else:
            toprint += " "

        toprint += "\n";

        logger.info(toprint);

        # Put the record into the Firehose stream
        try:
            self.firehose_client.put_record(DeliveryStreamName=self.firehose_name,
                                       Record={'Data': toprint})
        except ClientError as e:
            logging.error(e)
            exit(1)

    def on_error(self, status):
        logger.error(status)

def main():
    api = create_api()
    tweet_listener = TweetListener(api)
    stream = tweepy.Stream(api.auth, tweet_listener)
    stream.filter(track=["#covid"], languages=["en"])

if __name__ == "__main__":
    main()
