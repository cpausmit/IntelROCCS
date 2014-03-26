#IntelROCCS -- Intelligent Resource Optimization for Complex Computing Systems

##Conceptual Description of the Cache Release Mechanism

### Some Initial Remarks

For most purposes we consider datasets as our basic unit of data but in principle the unit could be data blocks. Presently the majority of data we are considering on the Tier-2 and Tier-1 sites is stored in complete datasets which means all blocks of a given dataset are at the specific site.

### Motivation

CMS maintains a huge amount of data, O(50PB), which is conveniently ordered in datasets, which usually have some common properties and user tend to analyse entirely. Datasets are further subdivided into data blocks which each consists of one or more files. These subdivisions facilitates the handling of the datasets.

The CMS computing system is literally distributed around the world in over 50 computing sites. Managing the attached disk space and making datasets available for analysis thus is a major enterprise. A convenient set of tools exists to move data around but there is no central intelligence to copy popular data to particular sites or remove less popular data from others. At present the various physics group each have a data manager who is responsible to manage the disk space assigned to the particular physics group. Usually this involves the management of about three Tier-2 computing sites and about a thousand datasets (mostly Monte Carlo simulation). Placement of new data is relatively straight forward but the deletion of older sets can be quite complicated and requires significant amount of time.

The automatic cache release process is supposed to optimize the usage of all available disk storage and relieve the data managers of a large fraction of their work by:

   0 keeping all storage filled to a high and safe level
   0 always allow new data to be received at any site
   0 remove the least valuable data from the storage when the storage fills over a given level

### Cache Release Process

The cache release is an operation that is performed on a given site separately but has to account for the status of the global CMS storage. Therefore it is an operation that has to be globally managed. The system is periodically reviewed to ensure there is free space for transfers at all sites. Usually little should have changed in the storage since the last review, so the first step will be to see whether cache release has to be triggered at any given site. The condition for the cache release is given by:

   * usedSpace / availableSpace > 90% (number can be adjusted)

If there is no site where cache release has to be triggered the cycle is completed.

In case any single site needs to release some data from its cache the full process, considering the availability of datasets at all sites, has to run. For the cache release process we need to consider a global view of all sites involved (Tier-1-buffer, Tier-2). This global view consists of all official datasets that are in the publicly managed disk space, indicating whether they are custodial or not. Custodial datasets cannot be touched by the cache release and are therefore immediately excluded from further considerations. Custodiality of a datasets is an information stored in the database and rarely changes (static).

Further we have to ensure that all datasets are available at at least two (also this number can be changed) sites. The so-called 'last datasets' have to be protected from deletion. The 'last dataset' information is not tracked in the database but it is something our process can and has to dynamically determine.

Before we start to consider datasets for deletion we therefore have to exclude the 'last datasets' from our lists. If datasets are more than two times available in the system we have to make a decision which of the copies are the last two copies. The last copies are chosen to be in the site with the largest available free space. Our goal is to distribute the last datasets as evenly among sites as possible. This is not the only valid strategy, there are other strategies that can be used.

With the custodial datasets excluded and the the last dataset copies determined and also excluded from deletion we have a list of datasets per site that are eligible for cache release (deletion). At any site where the cache release algorithm is triggered we have to identify enough datasets to restore the target of free space at the site:

   * usedSpace / availableSpace < 85% (number can be adjusted)

To select the datasets for deletion we apply a ranking algorithm that assigns each dataset a rank. We will select datasets from the top of the descending ranked list adding the space they occupy until we have enough space to meet the above condition. The selected datasets are then removed from the storage. The ranking algorithm is used to evaluate the usefulness of the data. The ranking (floating point number) is roughly equivalent to the number of days passed since the dataset was last used. This algorithm ensures that datasets that are unpopular will automatically disappear from all sites in the system, except for the last and custodial copies of course. Last copies and custodial datasets have to be removed with other tools.


### Data Ranking Algorithm

The ranking algorithm assigns each dataset a rank. The rank is roughly corresponding the the number of days the particular dataset has not been used. Thus the larger the rank the less useful are the data as they have not been accessed in so many days. There are a number of corrections applied to the number of days not used to account for the fact how often the dataset was used before or for how long it has been sitting on the disk to begin with, etc.

Here is the ranking formula [a first shot]:

<pre>
   rank = (1 - l_Used) * (t_Now - t_Created) + l_Used * (t_Now - t_LA - n_Access/size) - size/100

      t_Created  - date/time the dataset appeared on the site
      t_LA       - last access date/time
      t_Now      - current date/time
      l_Used     - logical whether sample was used at all (0 - not used, 1 - used)
      n_Access   - number of times sample was used
      size       - sample size in GB
</pre>
