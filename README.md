# AS_Mysql - Query AeroSpike from your favorite MySQL Client

This module implements the MySQL server protocol, so you will be able to query AeroSpike from your favorite MySql-Client.

AS_Mysql was intentionally written as a single python file, due to the following reasons:

- AeroSpike already has python installed.
- Running the program on production environments will be straightforward (for DevOps).

**Simple execution :**

- just run : `nohup python36 ./as_mysql.py  >/dev/null 2>&1 &`

**Contributers:**

- Zac Sadan
- ?

**Screenshots:**


*Connect:*

<img src="README/connect.png" height="300">

*Browse Namespaces:*

<img src="README/namespaces.png" height="300">

*Browse Variables:*

<img src="README/variables.png" height="300">

*Show Table Status (Table fields and details):*

<img src="README/table_status.png" height="300">

* 'Select' from AeroSpike :*

<img src="README/select.png" height="300">





Misc:

- Some of the code is ported version of : https://github.com/CloudQuote/faux-mysql-server/blob/master/src/index.js
- Windows Debugging: `/cygdrive/c/Program\ Files\ \(x86\)/Python36-32/python.exe -u as_mysql.py`
