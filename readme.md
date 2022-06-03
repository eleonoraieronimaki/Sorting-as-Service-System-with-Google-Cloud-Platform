# INSTRUCTIONS

## 1. Install flask with pip in environment

## 2. Install google libraries in environment
```
https://cloud.google.com/datastore/docs/datastore-api-tutorial?hl=en_US#python
```
1. In the link, you download the zip file and extract (if you already have an environment, no need to recreate one)
2. Follow instructions to install the requirements in your environment

## Running
1. In order to run the app, you need to set an environment variable so that the app can connect to the google cloud project and access the data.
https://cloud.google.com/docs/authentication/production </br>
The service account is created, you just need to download the json for it and use the export command given by google to set the location of the json. This only takes effect in the current terminal session. If you close the terminal, the variable must be set again.

## Files

*   ```
    web.py -> Contains the current web application. Run with:
        python web.py
    ```

*   ```
    templates/
    Contains the current templates for the web application. Currently very basic.
    ```

*   ```
    Dockerfile
    Run:
        docker build -t flask-app .
    Creates an image named flask-app for the current web application.
    ```

*   ```
    requirements.txt
    Contains the requirements for the flask app.
    ```

*   ```
    datastore.py
    Contains functions that are used in the web.py in order to store documents in Google Datastore
    ```

*   ```
    main.py
    Contains a sample google cloud functions which can be run locally following google's instructions found here:
    https://cloud.google.com/functions/docs/running/function-frameworks
    ```
