To compile the project:

1) open in Eclisle
2) set HBASE_HOME  class variable to hbase install dir


setup hbase tables:
---
open hbase shell
	$ hbase shell
	  	create 'access_logs', 'details'
	  	create 'summary_user', {NAME=>'details', VERSIONS=>1}
	  	
'access_logs' is the 'raw' logs.  The key is userID+counter  (int + int)
'summary_user' is to compute summary.  key is 'userID' (int)