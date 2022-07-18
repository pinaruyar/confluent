# Data Streaming Pipeline
 An end-to-end streaming pipeline using Confluent Cloud and ksqlDB.
 Project consists of python based producer and consumer applications along with the JSON schemas that will be needed.
 
 
## Goal

The goal is the create a [producer](https://docs.confluent.io/platform/current/clients/producer.html) to write metmuseum data to the topic that is created in Confluent Cloud and create [consumer](https://docs.confluent.io/platform/current/clients/consumer.html) to be able to read the data from that topic. 


## Requirements

#### In order to move on you will need to have an account on https://confluent.cloud

- Python3
- [ksqldb](https://docs.confluent.io/cloud/current/get-started/index.html)
- [Confluent CLI](https://docs.confluent.io/confluent-cli/current/overview.html)



## Setup

- create an account on Confluent Cloud
- follow the documentation and tutorials on the Confluent website in order to create a cluster (`art`) and a topic (`artworks`)
- clone this repository to your desktop 
- Install Confluent CLI in order to be able to manage your producer and consumer easily
- run producer.py
```
chmod u+x producer.py 
./producer.py config.ini
```
- you can always check if the messages being written below the topic you created in the messages section in your Confluent Cloud account
- you can run the consumer.py
```
./consumer.py config.ini
```


## Data Source

[Met Museum](https://metmuseum.github.io/)
 

## Debugging

For this purpose you can use two different command line pages, one with running your producer to write the data and one with consumer to see if the data has been written to results. 
Additionally a function(`delivery_callback()`) in producer.py gives you the real-time feedback of the status.



