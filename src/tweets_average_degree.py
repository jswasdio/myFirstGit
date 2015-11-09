##------------------------------------------------------------------------------
## Name:        js_tweets_average_degree
## Purpose:     Calculate simple average_degree of graph
## Author:      jswasdio@gmail.com
## Date:        11/08/2015
##------------------------------------------------------------------------------
"""
This program calculate average degree of graph.

The average degree is calculated by summing the degrees of all nodes in all graphs
and dividing by the total number of nodes in all graphs.

This program expects an input file in batch every 60 second. New file will overwritten existing file.
Once input file is processed , processed file is written out to a new file called as previous file to use
for next run.

Only tweets in previous file that are not older than 60 seconds and tweets in current file will be used
to calculate next average degree.

Assume this batch run every 60 seconds (ideally, this should be real-time process but I do not how set up API
to get tweets feed. There are still more details to work on to make sure that this program finish within 60 seconds and
we actually process all tweets and do not miss any of them).

Input
Example of tweets and time stamp in list are shown  per example below.
For current file, it is a jason dump from cleaned_tweet.py,
  current_tweets:
      [[['trump', 'election'], ['Fri Oct 30 15:29:45 +0000 2015']],
      [['deterrorenlabomba'], ['Fri Oct 30 15:29:45 +0000 2015']],
      [['pumpkin', 'halloween', 'happyhalloween', 'happyfriday'], ['Fri Oct 30 15:29:45 +0000 2015']]]

For previous file, it is a copy for current file of a previous run.
      previous_tweets:
      [[['trump', 'election', 'news'], ['Fri Oct 30 15:28:00 +0000 2015']],
      [['trump', 'election'], ['Fri Oct 30 15:28:45 +0000 2015']],
      [['diadelosmuertos'], ['Fri Oct 30 15:28:45 +0000 2015']]]

Output
Dictionary of edges and time can be generated from valid tweets(<= 60 seconds old) as shown below
    {('happyhalloween', 'happyfriday'): ['Fri Oct 30 15:29:45 +0000 2015'],
    ('trump', 'election'): ['Fri Oct 30 15:29:45 +0000 2015'],
    ('halloween', 'happyfriday'): ['Fri Oct 30 15:29:45 +0000 2015'],
    ('pumpkin', 'halloween'): ['Fri Oct 30 15:29:45 +0000 2015'],
    ('pumpkin', 'happyfriday'): ['Fri Oct 30 15:29:45 +0000 2015'],
    ('pumpkin', 'happyhalloween'): ['Fri Oct 30 15:29:45 +0000 2015'],
    ('halloween', 'happyhalloween'): ['Fri Oct 30 15:29:45 +0000 2015']}


total nodes = 6 (unique nodes in dictionary key)
total edges = 7 (length of dictionary)

In this case summing the degrees of all nodes in all graphs is equivalent to summing the number of edges
average degree = (7/6) = 1.17

"""
import string
import json
import re
import os
import shutil
from datetime import datetime


# refers to valid comment
## refers for testing only, not comments

def get_cleaned_tweets_tmp(filename):

    """
    Get cleaned tweets from json dump file or previous file

    Parameter:
        filename(string)

    Returns:
        List of cleaned tweets
        Example:
          current_tweets:
          [[['trump', 'election'], ['Fri Oct 30 15:29:45 +0000 2015']],
          [['deterrorenlabomba'], ['Fri Oct 30 15:29:45 +0000 2015']],
          [['pumpkin', 'halloween', 'happyhalloween', 'happyfriday'], ['Fri Oct 30 15:29:45 +0000 2015']]]

          previous_tweets:
          [[['trump', 'election', 'news'], ['Fri Oct 30 15:28:00 +0000 2015']],
          [['trump', 'election'], ['Fri Oct 30 15:28:45 +0000 2015']],
          [['diadelosmuertos'], ['Fri Oct 30 15:28:45 +0000 2015']]]
    """

    #get data from current tweet file or previous tweet file, depending on file name parameter
    tweet_clean_list_tmp =[]
    if os.path.isfile(filename) and (os.stat(filename).st_size > 0):
        with open(filename) as file_tmp:
            tweet_clean_list_tmp = json.load(file_tmp)
    return tweet_clean_list_tmp

def get_cleaned_tweets(cleaned_tweet_list_curr, cleaned_tweet_list_prev):

    """
    Get current list of cleaned tweets and tweets from previous run.
    Identify valid clean tweets from both list that are valid within 60 seconds of the most current tweets

    Parameter:
        cleaned_tweet_list_curr(list)
        cleaned_tweet_list_prev(list)
        example:
           current_tweets:
          [[['trump', 'election'], ['Fri Oct 30 15:29:45 +0000 2015']],
          [['deterrorenlabomba'], ['Fri Oct 30 15:29:45 +0000 2015']],
          [['pumpkin', 'halloween', 'happyhalloween', 'happyfriday'], ['Fri Oct 30 15:29:45 +0000 2015']]]

          previous_tweets:
          [[['trump', 'election', 'news'], ['Fri Oct 30 15:28:00 +0000 2015']],
          [['trump', 'election'], ['Fri Oct 30 15:28:45 +0000 2015']],
          [['diadelosmuertos'], ['Fri Oct 30 15:28:45 +0000 2015']]]


    Returns:
        List of valid cleaned tweets combined from current and previous list.
        This is contain records that has time stamp <=60 seconds from time of latest tweets)
        [[['trump', 'election'], ['Fri Oct 30 15:29:45 +0000 2015']],
        [['deterrorenlabomba'], ['Fri Oct 30 15:29:45 +0000 2015']],
        [['pumpkin', 'halloween', 'happyhalloween', 'happyfriday'], ['Fri Oct 30 15:29:45 +0000 2015']],
        [['trump', 'election'], ['Fri Oct 30 15:28:45 +0000 2015']],
        [['diadelosmuertos'], ['Fri Oct 30 15:28:45 +0000 2015']]]

    """

    #Remove tweets from previous file that are older than 60 seconds comparing to latest tweet in current file
    cleaned_tweet_prev_valid =[]
    cleaned_tweet_prev_combined =[]
    cleaned_tweet_final =[]
    for cleaned_tweets_curr in cleaned_tweet_list_curr:
       max_time_curr= max(dt_curr for dt_curr in (cleaned_tweets_curr[1]))
       max_t = datetime.strptime(max_time_curr, "%a %b %d %H:%M:%S +0000 %Y")

    for cleaned_tweets_prev in cleaned_tweet_list_prev:
        for dt_prev in cleaned_tweets_prev[1]:
            prev_t = datetime.strptime(dt_prev, "%a %b %d %H:%M:%S +0000 %Y")
            diff = max_t - prev_t
            if diff.seconds <= 60:
               cleaned_tweet_prev_valid = [cleaned_tweets_prev]
               cleaned_tweet_prev_combined.extend(cleaned_tweet_prev_valid)

    for prev_valid_tweets in cleaned_tweet_prev_combined:
        cleaned_tweet_list_curr.append(prev_valid_tweets)
    return  cleaned_tweet_list_curr


def generate_graph(node_list):

    """
    Generate all possible connected nodes in the same tweet line from list of hashtag

    Parameter:
        node_list(list)
        example:
        [[['trump', 'election'], ['Fri Oct 30 15:29:45 +0000 2015']],
        [['deterrorenlabomba'], ['Fri Oct 30 15:29:45 +0000 2015']],
        [['pumpkin', 'halloween', 'happyhalloween', 'happyfriday'], ['Fri Oct 30 15:29:45 +0000 2015']],
        [['trump', 'election'], ['Fri Oct 30 15:28:45 +0000 2015']],
        [['diadelosmuertos'], ['Fri Oct 30 15:28:45 +0000 2015']]]


    Returns:
        dictionary of all possible connected node (undirected graph)
        example:

        {'trump': ['election'], 'election': []}
        {'deterrorenlabomba': []}
        {'pumpkin': ['halloween', 'happyhalloween', 'happyfriday'],
        'halloween': ['happyhalloween', 'happyfriday'],
        'happyhalloween': ['happyfriday'],
        'happyfriday': []}
        {'trump': ['election'], 'election': []}
        {'diadelosmuertos': []}

    """

    graph = {}

    #generate all possible connected node in the same tweet line
    for i in range(len(node_list)):
        connected_node_all =[]
        for j in range(i+1,len(node_list)):
            connected_node_all.append(node_list[j])
        graph[node_list[i]]= connected_node_all
    return graph


def generate_edges(graph):
    """
    Generate all possible edges from connected nodes

    Parameter:
        graph(dictionary)
        example:
        {'trump': ['election'], 'election': []}
        {'deterrorenlabomba': []}
        {'pumpkin': ['halloween', 'happyhalloween', 'happyfriday'],
        'halloween': ['happyhalloween', 'happyfriday'],
        'happyhalloween': ['happyfriday'],
        'happyfriday': []}
        {'trump': ['election'], 'election': []}
        {'diadelosmuertos': []}

    Returns:
        List of edges
        example:
        [('trump', 'election')]
        []
        [('happyhalloween', 'happyfriday'), ('pumpkin', 'halloween'), ('pumpkin', 'happyhalloween'),
        ('pumpkin', 'happyfriday'), ('halloween', 'happyhalloween'), ('halloween', 'happyfriday')]
        [('trump', 'election')]
        []
    """

    #generated all egdes for all given nodes
    #assume bidirection edges except self direction
    edges = []
    for node in graph:
        for connected_node in graph[node]:
            edges.append((node, connected_node))

    return edges


def calculate_average_degree(tweet_clean_list):
    """
    Calculate average degree of graph
    average degree = (number of unique nodes)/(sum of the degrees of all nodes in all graphs)
    which is
    average degree = (number of unique nodes)/(sum of edges of all possible connected node in all graphs)

    Parameter:
        tweet_clean_list(list)
        example:
        [('trump', 'election')]
        []
        [('happyhalloween', 'happyfriday'), ('pumpkin', 'halloween'), ('pumpkin', 'happyhalloween'),
        ('pumpkin', 'happyfriday'), ('halloween', 'happyhalloween'), ('halloween', 'happyfriday')]
        [('trump', 'election')]
        []
{

    Returns:
        average degree of graph(number)

        example
         unique number of node =6
         {i.e 'happyhalloween', 'election', 'happyfriday', 'halloween', 'trump', 'pumpkin'}

         sum of number of edges = 7
         {('pumpkin', 'halloween'): ['Fri Oct 30 15:29:45 +0000 2015'],
         ('pumpkin', 'happyfriday'): ['Fri Oct 30 15:29:45 +0000 2015'],
         ('halloween', 'happyfriday'): ['Fri Oct 30 15:29:45 +0000 2015'],
         ('halloween', 'happyhalloween'): ['Fri Oct 30 15:29:45 +0000 2015'],
         ('trump', 'election'): ['Fri Oct 30 15:29:45 +0000 2015'],
         ('happyhalloween', 'happyfriday'): ['Fri Oct 30 15:29:45 +0000 2015'],
         ('pumpkin', 'happyhalloween'): ['Fri Oct 30 15:29:45 +0000 2015']}

         average degree = (7/6) = 1.17

    """
    node_set = set()
    edge_time_dict = {}
    edge_time_set =set()

    for nodes_time in tweet_clean_list:
        for edges in  generate_edges(generate_graph(nodes_time[0])):
            # if edges exists, append all valid time for that edges
            if edges in edge_time_dict:            
               edge_time_dict[edges].append(nodes_time[1])
            else:
               edge_time_dict[edges] = [nodes_time[1]]
            edge_time_set.add(edges)
    ##for keys in edge_time_dict:
    ##   print('dict',keys, edge_time_dict[keys], max(edge_time_dict[keys]))

    # get the most current time for each edge, same edge can occurs at different times
    for edge_time in edge_time_dict:
        edge_time_dict[edge_time] = max(edge_time_dict[edge_time])
        # create all valid node set, only nodes the has edge(s) are included in node set
        for nodes in edge_time:
            node_set.add(nodes)

    #calculate average_degree
    if node_set:
       average_degree = round(len(edge_time_dict)/len(node_set),2)
    else:
       average_degree = 0
    print(len(edge_time_dict), len(node_set), average_degree)
    return average_degree

def generate_output(average_degree,outputfile2):
    with open (outputfile2, 'a') as output_file_avg:
        output_file_avg.write(str(average_degree)+'\n')

def backup_files(src,target):
    # copy file from source to target
    shutil.copyfile(src, target)
    
def main():
    #Define input and output path
    #To simplify, they are all written to input directory
    #Restart and recovery are not in scope.
    tmp_current_input_file_path = (r".."+ os.sep + r"tweet_input"+ os.sep+ r"ft1_tmp_curr.txt")
    tmp_previous_input_file_path = (r".."+ os.sep + r"tweet_input"+ os.sep+ r"ft1_tmp_prev.txt")
    tmp_backup_input_file_path = (r".."+ os.sep + r"tweet_input"+ os.sep+ r"ft1_tmp_backup.txt")
    output_file_path_2 = (r".."+ os.sep+ r"tweet_output"+os.sep + r"ft_2.txt")

    #Assume tweets comes in a text file and has been cleaned by previous step and are written out to a list
    #and was dump as a json file.

    #Read current tweets file
    tweets_curr = get_cleaned_tweets_tmp(tmp_current_input_file_path)

    #Read previous tweets file(file that has been process for a previous run)
    tweets_prev = get_cleaned_tweets_tmp(tmp_previous_input_file_path)

    #Filter valid tweets based on 60 seconds lag time
    tweets_valid = get_cleaned_tweets(tweets_curr, tweets_prev)

    #Calculate average_degree and write output to file
    generate_output(calculate_average_degree(tweets_valid),output_file_path_2)

    #this copy is for debugging purpose only.
    #shutil.copyfile(tmp_previous_input_file_path, tmp_backup_input_file_path )

    #copy current file to previous file for next run
    backup_files(tmp_current_input_file_path,tmp_previous_input_file_path)

    print('End of js_tweets_average_degree program')

if __name__ == '__main__':
    main()