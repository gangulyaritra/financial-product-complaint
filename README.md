# Financial Product Complaint using PySpark and CircleCI on Google Cloud Platform.

### Problem Statement.

Complaints provide insights into problems that a customer experiences in the marketplace and help to understand the reason for necessary modifications in the existing financial product if required.

By understanding existing complaints registered against financial products, we can apply machine learning to identify newly registered complaints, whether they are problematic or not, and accordingly, take quick action to resolve the issue and satisfy the customer's need. The task is to identify whether or not the registered complaint will get disputed by the customer.

### Dataset.

Explore the [Consumer Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/) of Financial Products and Service complaints to see how companies respond to consumers. View trends, see maps, read complaints, and export the data.

Download the dataset from this [**[LINK]**](https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?date_received_max=2022-11-25&date_received_min=2021-11-25&field=all&format=json). Hundreds of complaints regarding financial product services get added/updated every day on a real-time basis on this website.

### Tech Stack & Infrastructure.

1. PySpark (Spark SQL & MLlib)

2. MongoDB Atlas

3. Apache Airflow

4. Dashboard (Prometheus, Grafana, Promtail, Loki, Node Exporter)

5. AWS S3 Bucket for Artifact Registry.

6. GCP Container Registry to store Docker Images.

7. GCP Compute Engine to deploy the application.

8. CircleCI for CI/CD Pipeline.

#### Step 1: Install PySpark locally OR access PySpark on Neuro Lab.

#### Step 2: Create Virtual Environment and Install Dependency.
```bash
pip install -r requirements.txt
```

#### Step 3: Create a .env file and Paste the Environment Variables.
```bash
=========================================================================
Paste the following credentials as system environment variables.
=========================================================================

MONGO_DB_URL="mongodb+srv://root:root@fpc-db.znfajpt.mongodb.net/?retryWrites=true&w=majority"
AWS_ACCESS_KEY_ID=AKIAV5L23LTSB3GNK5OV
AWS_SECRET_ACCESS_KEY=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
TRAINING=1
PREDICTION=1
```

#### Step 4: Run the Application Server.
```bash
python main.py --t=1	[Start Training Pipeline]

python main.py --p=1	[Start Prediction Pipeline]
```

#### Step 5: Build and Launch the Docker Image.
```bash
docker build -t fpc-img:lts .

docker run -it -v $(pwd)/fcp:/app/fcp  --env-file=$(pwd)/.env fpc-img:lts
```

#### Step 6: To start the application.
```bash
docker-compose up
```

#### Step 7: To stop the application.
```bash
docker-compose down
```

### Apache Airflow Setup.

Only Linux and Mac support Apache Airflow Setup.

#### Set Airflow Directory.
```bash
export AIRFLOW_HOME="/home/dimpu/VSCode/financial-product-complaint/airflow"
```

#### Install Airflow.
```bash
pip install apache-airflow
```

#### Configure Database.
```bash
airflow db init
```

#### Create a User for Airflow Dashboard.
```bash
airflow users create  -e aritraganguly.msc@protonmail.com -f Aritra -l Ganguly -p admin -r Admin  -u admin
```

#### Start Scheduler.
```bash
airflow scheduler
```

#### Launch Airflow Server.
```bash
airflow webserver -p <port_number>
```

#### Update in airflow.cfg
```bash
enable_xcom_pickling = True
```

## Authors

- [Aritra Ganguly](https://in.linkedin.com/in/gangulyaritra)

## License & Copyright

© 2022 Aritra Ganguly, iNeuron.ai

Licensed under the [MIT License](LICENSE).