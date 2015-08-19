## CUADRnT - CMS Usage Analytics and Data Replication Tools
"Quadrant" -> /ˈkwädrənt/

A collection of tools to analyze data usage behavior in the CMS experiment and make intelligent decisions to replicate data based on learned information. Collects information from a number of CMS tools including but not limited to PhEDEx, Popularity DB, and CRAB3. Also includes a visualization tool for easier understanding of current system status and past system usage.

Goal is to not only recognize, but predict popularity of a dataset based on previous user behavior using machine learning. System is kept balanced using a novel algorithm called "Rocker Board Algorithm" which distributes workload throughout the whole system, "balancing the rocker board", to avoid unbalanced force distribution which could cause it to "tip".

Services such as access to PhEDEx, Popularity DB, CRAB3 etc. are currently kept in a seperate package but are needed by CUADRnT.

### Popularity Index
Generate a popularity index for each dataset indicating if the dataset is about to become popular, is stable, or will soon decrease in popularity. This is the main part and is currently under development. Future work will include a machine learning algorithm for improved popularity prediction.

```python
def getPopularity(datasetName):
    popularity = 0
    today = datetime.today()
    oldAccesses = []
    for i in range(8, 15):
        date = today - datetime.timedelta(days=i)
        oldAccesses.append(getDatasetAccesses(date, datasetName))
    for i in range(1, 8):
        date = today - datetime.timedelta(days=i)
        newAccess = getDatasetAccesses(date, datasetName)
        for oldAccess in oldAccesses:
            popularity += newAccess - oldAccess
    return popularity
```

### Rocker Board Algorithm
Based on the popularity index datasets are ranked based on their relative deviance per size unit. A positive deviance means the dataset is going to become more popular than the average dataset and a negative deviance means the dataset is going to decrease in popularity more than the average dataset. A 0 rank means the dataset is stable, this should be the most common scenario. The rank is normalized over size to balance utilization of sites as each site is limited by its available space and should have an equal amount of accesses per size unit. To rank sites the ranks for all replicas at each site are summed up to get a measurement for how popular all datasets at the site will become. Work is in the progress to improve the site ranking by including the total amount of CPU's available at the site. This way each site is treated based on it's individual resources and not just assumed all sites have an equal storage to CPU ratio.

```python
def getDatasetRankings(datasets):
    alphaValues = dict()
    for datasetName in datasets:
        nReplicas = getNumberReplicas(datasetName)
        sizeGb = getDatasetSize(datasetName)
        popularity = getPopularity(datasetName)
        alpha = float(popularity)/(nReplicas*sizeGb)
        alphaValues[datasetName] = alpha
        mean = (1./len(alphaValues))*sum(v for v in alphaValues.values())
    datasetRankings = dict()
    for k, v in alphaValues.items():
        dev = v - mean
        datasetRankings[k] = dev
    return datasetRankings
```

```python
def getSiteRankings(sites, datasetRankings):
    siteRankings = dict()
    for siteName in sites:
        datasets = getAnalysisOpsDatasetsAtSite(siteName)
        rank = sum(datasetRankings[d] for d in datasets)
        siteRankings[siteName] = rank
    return siteRankings
```

### Instant Replication Algorithm
In addition to the rocker board algorith which is ran once a week there is a daily algorithm which replicates datasets which are in need of instant replication. These are decided based on data from CRAB3. If a job has been in queue for more than 1 day and more than half of the tasks are left the dataset on which the job is analysing will be instantly replicated to a site with available space which does not have a job stuck in othe queue.

### Required programs

* Python 2.7
* mongodb >= 3.0
* MySQL >= 5.7

### Required Python packages
* pymongo
* MySQLdb

### INSTALL
IMPORTANT! - You must update /etc/setup.cfg 'username' and 'group' values to the username and group of that user for the user which will run the scripts. This is needed to correctly set up permissions for log and data paths.

Install package by running as sudo:
~$ python setup.py install

Run tests as user which will run the code
~$ python setup.py test

mongodb server does not have to be started explivitly as this is taken care of in storage module. However if needed a bin file start_mongodb is installed and can be executed from the command line.
the mongodb server however is not automatically stopped as to not risk issues with other running services. Therefore a bin file stop_mongodb is install which can be executed from the command line to stop server.
Can change the bin file to change where database is stored.

Run:
$ sudo python setup.py build
$ sudo python setup.py test
$ sudo python setup.py install
