# Webgraph

This project provides a hands-on understanding of `Google Cloud Dataproc`, `Apache Spark`, `Spark SQL`, and `Spark Streaming over HDFS`. 
In this project, we are going to 
- 🛠️ Deploy Spark and HDFS
- 🌐︎ Write a Spark program generating a web graph from the entire Wikipedia
- 📊 Write a PageRank program to analyze the web graph

## Environment Setup
The project is completed on **`Google Cloud Dataproc`**, which is a managed cloud service for Spark and Hadoop, allowing users to spin up Spark and Hadoop instances quickly.

### Google Cloud Platform
Use the following [link](https://console.cloud.google.com/dataproc), and click on *Enable API* to enable Google Cloud Dataproc. Click on *Create cluster* to create a Dataproc Cluster. Notice there are three Cluster modes you could choose from: "Single Node", "Standard", and "High Availability".

### Jupyter Notebook
Google provides an easy interface to install and access Jupyter Notebook when creating Dataproc cluster. Go down to **Component gateway** and select **Enable access to the web interfaces of default and selected optional components on the cluster**. Then click on Advanced options. Go down to **Optional components** and click on *Select component*. In the pop-up window, choose Anaconda and Jupyter Notebook.

### Cluster properties
Components like Spark and Hadoop have many configurations that users could tune. Users can change the default values of those settings when creating the Dataproc cluster. For example, if you want to change the default block size of HDFS to 64MB, you can change it by adding a cluster property under **Cluster properties** in the Advanced Options.

## 🔘 Webgraph on Internal Links
This script takes an xml file as input and generates a csv file that describes the webgraph of internal links in Wikipedia. The csv file contains a list of articles and the articles they link to, as shown below:


| A  | B |
| ------------- | ------------- |
| article_1 | article_0 |
| article_1 | article_2 |
| article_1 | article_3 |
| article_2 | article_3 |
| article_2 | article_0 |
...


## 🔘 Spark PageRank
This script implements the PageRank algorithm, which is used by Google to rank websites in the Google Search, to calculate the rank of articles in Wikipedia. The algorithm has the following steps:

1. Each article is assigned an initial rank of 1.
2. On each iteration, the contribution of an article A to its neighbor B is calculated as its rank divided by the number of neighbors.
3. The rank of the article B is updated to be 0.15 + 0.85 * contribution.
4. The algorithm repeats the above steps until convergence.
