# Follow the steps to run the job

Assuming all these Pre-requisite are installed on the system.

- Install Homebrew
- Install Java
- Install Python
- Install PySpark

### 4. Install PySpark

PySpark is a Spark library written in Python to run Python applications using Apache Spark capabilities.

```sh
# Install Apache Spark
brew install apache-spark
```

This installs the latest version of Apache Spark which ideally includes PySpark.
After successful installation of Apache Spark run pyspark from the command line to launch PySpark shell.

# Running the Job

If the pre-requisite already installed then you can directly clone this repository.

```sh
# clone the repository
git clone https://github.com/harsh2102/hrs-etl.git
cd bookings-etl
```

after cloning this repository run the follwoing command to run the script :

```sh
# Stand-alone mode
spark-submit --deploy-mode client ./hotel.py
# Cluster mode
spark-submit --deploy-mode cluster ./hotel.py
```

> cluster mode will not run on your local system for that you'll require a multi machine cluster.

After this this script it'll create the output folder in the current directory with the name of <i>ETL_hotel_df</i> which will contains the all the output files in the parquet format.
