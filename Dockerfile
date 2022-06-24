# start by pulling the python image
FROM python:3.9-alpine

# copy the requirements file into the image
COPY requirements.txt /app/requirements.txt

# switch working directory
WORKDIR /app

RUN apk add --no-cache --virtual .build-deps g++ gcc libc-dev libxslt-dev linux-headers && \
    apk add --no-cache libxslt && \
    pip install --no-cache-dir lxml>=3.5.0

# install the dependencies and packages in the requirements file
RUN pip install -r requirements.txt

# copy every content from the local file to the image
COPY ./src /app

# configure the container to run in an executed manner
ENTRYPOINT [ "python" ]

CMD ["web.py" ]