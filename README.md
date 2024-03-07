## Large Language Model (LLM) Processing Pipeline Project
## Authors:
- Freeman Chen 
- Abhi Erra
- Amy Cho
- Karthik Ayyalasomayajula
- Ronel Solomon
---
## Intro:
**"Ever clicked on a headline so compelling that you just couldn't resist, only to find out the story was about as exciting as watching paint dry? 🎨 Welcome to the world of clickbait, the internet's version of 'bait and switch.' 🎣 But what if we told you there's a way to sift through the sensational to find the substantial? 🕵️‍♂️ Enter our project: a large language model (LLM) processing pipeline that doesn't just read between the lines—it reads between the clicks. 👀**
![alt text](/Images/image.png)
---
### Project Description

This repository contains all necessary code, documentation, and resources used in our research for building and automating a large language model processing pipeline. It is designed to serve as a practical framework for analyzing text data at scale, specifically targeting the identification and comparison of clickbait content in political news.

---

### Getting Started

#### Dependences 
- Python 3.8
- Apache Spark
- MongoDB Atlas
- Apache Airflow
- Google Cloud Services (GCS)

#### installation
Clone the repository to your local machine
```
https://github.com/cho-amy/waffle-iron
```

#### Configuration and Execution
Refer to the individual guides within the repository for configuring and executing each component of the [pipeline](ml_pipline):

- [API_gcs.py](ml_pipline/API_gcs.py): Contains scripts for calling external APIs to gather data. Scripts and notebooks for cleaning and preprocessing raw text data.
- [aggregates_to_mongo.py](ml_pipline/aggregates_to_mongo.py): Documentation and configuration files for storing data in MongoDB Atlas.Manipulating and analyzing text data, including feature extraction and model training.
- [airflow_call.py](ml_pipline/airflow_call.py): Configuration files and scripts for automating the pipeline using Apache Airflow.

#### ML images :
