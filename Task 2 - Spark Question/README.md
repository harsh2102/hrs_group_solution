# Follow the steps to run the job

Assuming all these Pre-requisite are installed on the system.

- Install Java
- Install Python
- Install PySpark

# Running the Job

If the pre-requisite already installed then you can directly clone this repository.

```sh
# clone the repository
git clone https://github.com/harsh2102/hrs_group_solution.git
cd hrs_group_solution/Task\ 2\ -\ Spark\ Question/
```

after cloning this repository run the follwoing command to run the script :

```sh
# Stand-alone mode
spark-submit --deploy-mode client ./hrs_hotel.py
# Cluster mode
spark-submit --deploy-mode cluster ./hrs_hotel.py
```

> cluster mode will not run on your local system for that you'll require a multi machine cluster.

After running this script it'll create the output folder in the current directory with the name of <i>ETL_hotel_df</i> which will contains the all the output files in the parquet format.
