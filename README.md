Author: Jittima Swasdio 

Repository name: myFirstGit   

Please replace the existing tweet_input/tweets.txt file with the one you'd actually like to use.  The current one 
is a small example file that I used for testing these programs.

If the tweet_output directory does not exist, please create it.

The unicode handling in tweets_cleaned.py is NOT complete.  I partly cleaned up using decode(), but I didn't completely clean the data.  Also, I do not generate a count.  
The tweets_average_degree.py program will work properly if the input file has been cleaned.  I tested tweets_average_degree.py with the first 50 rows of the Insight data file (after cleaning them partially with tweets_cleaned.py), and it worked.  

Even though the first program does not do all the processing requested in the problem assignment, you must still run it because the output file of the first program is the input file for the second program.  

This solution might not scale well.  It is not designed to be parallelized across multiple computers.

If I had more time, I would also test with a wider variety of input data.
