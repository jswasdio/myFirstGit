##------------------------------------------------------------------------------
## Name:        js_tweets_average_degree
## Purpose:     clean tweets from file contain json records
## Author:      jswasdio@gmail.com
## Date:        11/08/2015
##------------------------------------------------------------------------------
#  This programs still need to clean up unicode properly and count unicode rows
##------------------------------------------------------------------------------

import string
import json
import re
import os
import shutil


def get_tweets(inputfile, outputfile, tmp_outputfile):

    """
    Get tweets from input file, which contain json data.
    Clean up and write out cleaned records to output file, ft1.txt
    Create list of clean tweets for further processing.

    Example of output file are shown below.
    Doing great work #Apache (timestamp: Thu Oct 29 17:51:55 +0000 2015)
    Excellent post on #Flink and #Spark (timestamp: Thu Oct 29 17:51:56 +0000 2015)

    Parameter:
        inputfile(string)
        outputfile(string)
        tmp_outputfile(string)

    Returns:
        List of cleaned tweets

    """

    tmp_tweet_raw =[]
    tweet_raw_list =[]
    hashtag_pattern = r'(#+\S+)'
    with open( inputfile, mode = 'r') as input_file:
         # to write cleaned tweets to output file, ft1.txt .
         with open(outputfile, 'w',) as output_file:
             for line in input_file:
                 parsed_json = json.loads(line)
                 parsed_json['text'] = parsed_json['text'].encode('ascii', 'ignore') #Need or not?
                 output_file.write(str(parsed_json['text'])+ ' (timestamp: '+str(parsed_json['created_at'])+')'+'\n')
                 #Generate list of hashtag and created time for further processing
                 tmp_hashtag = re.findall(hashtag_pattern,str(parsed_json['text']))
                 tmp_tweet_raw = [tmp_hashtag, [parsed_json['created_at']]]
                 if tmp_hashtag:
                     tweet_raw_list.append(tmp_tweet_raw)
    return(tweet_raw_list)

def cleanup_tweet(tweet_raw_list,tmp_outputfile):

    """
    Filter only hashtag and created time from list and written them out to list.
    Use json dump to dump list to intermediate file for further processing.


    Parameter:
        tweet_raw_list(list)
        tmp_outputfile(string)


    Returns:
        List of cleaned tweets
        Note: this program also written out list to file using json.dump
    """


    #write cleaned tweets to list and dump to json file for further processing
    tweet_clean_list =[]
    for tweet_list in tweet_raw_list:
        tmp_tweet_clean =[]
        tmp_hash_clean=[]
        for hashtag_raw in tweet_list[0]:
            hashtag_raw = hashtag_raw.strip(string.punctuation).replace(string.punctuation,'').lower()
            if hashtag_raw:
                tmp_hash_clean.append(hashtag_raw)
        tmp_tweet_clean =[tmp_hash_clean, tweet_list[1]]
        tweet_clean_list.append(tmp_tweet_clean)

    with open(tmp_outputfile, 'w') as output_file1:
         tweet_encoded =json.dump(tweet_clean_list, output_file1)

def main():
    output_file_path_1 = (r".."+ os.sep+ r"tweet_output"+os.sep + r"ft_1.txt")
    input_file_path = (r".."+ os.sep + r"tweet_input"+ os.sep+ r"tweets.txt")
    tmp_current_input_file_path = (r".."+ os.sep + r"tweet_input"+ os.sep+ r"ft1_tmp_curr.txt")
    tweets=get_tweets(input_file_path,output_file_path_1,tmp_current_input_file_path)
    cleanup_tweet(tweets,tmp_current_input_file_path)
    print('End of js_tweets_cleaned program')

if __name__ == '__main__':
    main()
