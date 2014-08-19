# IntelROCCS

Intelligent Resource Optimization for Complex Computing Systems

## Conceptual Description

CMS maintains a huge amount of data, O(50PB), which is conveniently ordered in datasets, which usually have some common properties and user tend to analyse entirely. Datasets are further subdivided into data blocks which each consists of one or more files. These subdivisions facilitates the handling of the datasets.

The CMS computing system is literally distributed around the world in over 50 computing sites. Managing the attached disk space and making datasets available for analysis thus is a major enterprise. A convenient set of tools exists to move data around but there is no central intelligence to copy popular data to particular sites or remove less popular data from others. At present the various physics group each have a data manager who is responsible to manage the disk space assigned to the particular physics group. Usually this involves the management of about three Tier-2 computing sites and about a thousand datasets (mostly Monte Carlo simulation).

There are two essential components in Data Management: there is the placement of data (new or very popular ones) and the deletion of older sets or simply less popular sets. Both components with the amount of datasets and the amount of sites are quite complicated and requires significant amount of time if performed by hand.

The automatic cache release process and the dynamic data placement are supposed to optimize the usage of all available disk storage and relieve the data managers of a large fraction of their work by:

* keeping all storage filled to a high and safe level
* always allow new data to be received at any site to be able to optimize data access
* remove the least valuable data from the storage when the storage fills over a given level

IntelROCCS is supposed to provide the necessary intelligence to optimize the resource usage at CMS.


### Components

So far implemented is the cache release. The package is called Detox (for site detoxification).
