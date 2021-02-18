In this Exercise we try to solve below problems using data accounts and logs data.

1. Using map-reduce, count the number of requests from each user.
(a) Use map to create a Pair RDD with the user ID as the key, and the integer 1 as the value. (The user ID is the
third field in each line.) Your data will look something like this:
(b) Use reduce sum the values for each user ID. Your RDD data will be similar to:

2. Use countByKey to determine how many users visited the site for each frequency. That is, how many users visited
once, twice, three times and so on.
(a) Use map to reverse the key and value, like this:
(b) Use the countByKey action to return a collection of frequency:user-count pairs.

3. Create an RDD where the user id is the key, and the value is the list of all the IP addresses that user has connected
from. (IP address is the first field in each request line.)
• Hint: Map to (userid,ipaddress) and then use groupByKey.
Join web log data with account data. The first field in each line is the user ID, which
corresponds to the user ID in the web server logs. The other fields include account details such as creation date, first and
last name and so on.

4. Join the accounts data with the weblog data to produce a dataset keyed by user ID which contains the user account
information and the number of website hits for that user.
(a) Create an RDD based on the accounts data consisting of key/value-array pairs: (userid,[values...])
Page 2
(b) Join the Pair RDD with the set of user-id/hit-count pairs calculated in the first step.
(c) Display the user ID, hit count, and first name (3rd value) and last name (4th value) for the first 5 elements, e.g.:
        userid1 4 Cheryl West
        userid2 8 Elizabeth Kerns
        userid3 1 Melissa Roman
