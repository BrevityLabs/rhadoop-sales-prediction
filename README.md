rhadoop-sales-prediction
========================

Predict sales for the customers with non-stationary purchase patterns.

The project is implemented in R with rmr2 of RHadoop package. The project is intended to run on AWS Elastic MapReduce environment. Because of the current version of Debian running on EMR, the version of R and rmr2 required, the entire instance setup process is manual and very precarious.

Setup process -
The instance is setup using a bootstrap process in which Squeeze release of R is installed and subsequent to that the dependencies and rmr2 are installed. The current versions can be seen in the bootstrap.sh script.

Steps -
Though I intend to call my R programs using EMR steps but currently I am unable to call the script files. I am using the EMR script runner called script-runner.jar. Unfortunately the error show that the shell script file could not be found. As a workaround, I am logging into the master node using SSH (Putty.exe) and running the R program.

The program outline goes like this -
* Read the input files which has customer ID, dates and amount in 3 columns
* Aggregate them by month for each customer and make time series
* Determine stationarity of these timeseries and pick out the non-stationary ones
* (not MR) Take each of the "pick-out" customers create pairs, N-to-N
* For each pair determine correlation to see if they influence each other
* Those showing positive strong correlation would be grouped together (currently missing)
* Find sales prediction using auto ARIMA

The program is written in 2 parts -
1. MR_Main.R - the main module with the program entry point
2. MR_Custom_Functions - Collection of simple routines used
       This includes a method which pulls out time series for a customer uses an MR function
       
