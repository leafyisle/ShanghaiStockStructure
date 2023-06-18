# Shanghai Stock Structure 

### System
* Linux. Verified on CentOS8, Debian Bullseye

### Features
* **Data Collection**, Collect the Semi-realtime stock trading data from hq.sinajs.cn and store the data in a MariaDB database
* **Data Retrieving**, Retrieve the data from the MariaDB database
* **Data Analysis**, do clustering using affinity propagation
* **Visualization**, present the visualization with embedding in 2D space

### Remark
* As a result of collecting the data in a semi-realtime manner, the data presented is also "closed" to "semi-realtime".
* With slight adjustment of the code, a distributed collection could be made to get more accuracy of the trading data.
