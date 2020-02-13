FROM python:3.7.6

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_REGION
ARG BATCHES
ARG FILES_PER_BATCH
ARG RECORDS_PER_FILE
ARG PREFIX
ARG BUCKET

WORKDIR /app

# Add source code to /app
ADD CDRGenerator.py /app
ADD country-codes.csv /app
ADD uk-area-code-list.csv /app

# Install python dependencies
RUN pip3 install numpy boto3

# Run command
CMD [ "python3", "/app/CDRGenerator.py" ]