# Cloud Computing - Assignment 2
## Andreas Savva - s3316491<br>
## Maria Ieronymaki - s3374831

## Description
This is the final assignment of the Cloud Computing course, Leiden University, 2022. In the asignment we had to create a sorting as a service system for files larger than 10MB, using the Google Cloud services. The application uses Flask for the front end which is deployed in Google Kubernetes Cluster and 4 Cloud Functions which are triggered using Pub/Sub messaging. In addition it uses Google Cloud Storage for storing the text files and Google Firestore as a database for job descriptions. We also provide an implementation using the Google Datastore db but it has conflict issues.

# INSTRUCTIONS

## Deploy the src_firestore application

## 1. Enable the following APIs and services in the Google Cloud
First create a project
Enable Pub/Sub, Cloud Functions, Kubernetes Engine, Datastore, Firestore, Cloud Storage,  Artifact Registry 

## 2. Create a bucket in the Cloud Storage
The bucket must be named 'saas-docs'

## Create 4 topics in the Google Pub/Sub service
1. upload_text
2. palindrome_worker
3. Reduce
4. palindrome_reducer

## Create a service account
Create a service account and give permissions for read and write to all the previous APIs. Download the JSON key and place it in the src folder

## Change the project ID
Replace the project id in the files with your own

## Use google cli 
* Install: https://cloud.google.com/kubernetes-engine/docs/quickstarts/deploy-app-container-image#python_1
* Initialize: https://cloud.google.com/sdk/docs/initializing
* Install the kubernetes component: gcloud components install kubectl

## Deploy the cloud functions
We deployed them using the google console which is very easy. Simply go in Cloud functions and create each function.
1. Select the region
2. Select as trigger type: Cloud Pub/Sub and for each function select the topics below:
    * sorting -> upload_text
    * palindrome -> palindrome_topic
    * reducer -> Reduce
    * palindrome_reducer -> palindrome_reducer
3. Select retry on failure, max instances and set timout to 300s.
4. Select python 3.9
5. Copy the contents of the function to the main.py in the cloud console.
6. Set the entry point as the function in each file. For example, for the sorting, the entry point is: sort_worker
7. Set the requirements. They are the same for all 4 functions and are contained in our files. cloud_functions/requirements.txt
8. Deploy.
The same can be achieved using the google cli in a terminal. https://cloud.google.com/functions/docs/deploying/filesystem

## Deploy the Kubernetes Cluster
Deploy the kubernetes cluster for the front end.
Go the the src folder
```
cd src_firestore/
```
```
gcloud config get-value project
```
At the next command, replace PROJECT_ID with your project id and LOCATION, to the desired one. For the locations you can use:
```
gcloud artifacts locations list
```
```
gcloud artifacts repositories create saas-repo \
    --project=PROJECT_ID \
    --repository-format=docker \
    --location=LOCATION \
    --description="Docker repository"
```
Build the image: (Replace LOCATION and PROJECT_ID)
```
 gcloud builds submit \
    --tag LOCATION-docker.pkg.dev/PROJECT_ID/saas-repo/saas-app .
```
Create a cluster (Replace COMPUTE_REGION)
```
 gcloud container clusters create-auto saas-app \
    --region COMPUTE_REGION
```
```
kubectl get nodes
```
Import credentials (Replace PATH-TO-KEY-FILE with the path to your JSON key)
```
kubectl create secret generic saas-key --from-file=key.json=PATH-TO-KEY-FILE.json
```
Deploy
```
kubectl apply -f deployment.yaml
```
```
kubectl get deployments
kubectl get pods
```
Deploy a load balancer
```
kubectl apply -f service.yaml
```
Copy the IP of the load balancer given below
```
kubectl get services
```
More detailed kubernetes deployment instructions: <br>
https://cloud.google.com/kubernetes-engine/docs/quickstarts/deploy-app-container-image#python_1


The application is now deployed

We also provide the requirements file in the main folder which can be used to run the front-end locally. 