#! /usr/bin/env Rscript

# Source name   : src_mapreduce/MR_MAIN.R
# Project Name  : Behaviour Pattern Recognition 
# Client name   : Yosef
# Author        : mukul.biswas.ie@ieee.org
# version       : Initial submission
#-----------------------------------------------------------------------------------------
# Purpose       : This is the main program which binds 3 modules each performing one stage
#                 of processing. The program runs as a batch job with no user intervention.
#                 The results are stored in data-files in the Hadoop DFS.
#                 The data files should have been loaded into the HDFS before the run.
#                 The program is logically broken down in 6 steps which each having one
#                 Map-Reduce module. The steps are -
#                   STEP 0 : Initialization, declaration, load libraries
#                   STEP 1 : Time-series preparation for all the customers data
#                   STEP 2 : Trend determination of the time series
#                   STEP 3 : Pairing of the deviant customers (for the next step) 
#                   STEP 4 : Grouping Customer with similar patterns (correlation)
#                   STEP 5 : Prediction of sales behavior (AUTO.ARIMA)
# 
#
# Note          : 1.The program can either be run in local mode or in DFS more (see the
#                   line tagged with >>>CYCLE0:2. In order to execute the program in Amazon
#                    AWS EMR, there is a separate set of instructions. Ask the author.
#                 2.The source code has tags like ">>>CYCLE0" and ">>>CYCLE1". The first
#                   set must be changed to suit a new run location/environment. The latter
#                   one are optional parameters to change the behavior of the program.
#                 3.The program must run inside an R environment. Additionally RHadoop pac-
#                   kage must be installed. A Hadoop environment, by itself, is not requir-
#                   ed because the program can as well run in the "local mode". Of course,
#                   with no scabalibity guaranateed in that case.
#------------------------------------------------------------------------------------------


# STEP 0 : Initialization, declaration & load libraries
#------------------------------------------------------------------------------------------

#Check details at https://github.com/RevolutionAnalytics/RHadoop/wiki/rmr
Sys.setenv(HADOOP_HOME="/home/hadoop")
Sys.setenv(HADOOP_CONF="/home/hadoop/conf")
Sys.setenv(JAVA_HOME="/usr/lib64/jvm/java-7-oracle/")

library(rmr2)

# Global variables and settings
gbl.deviants   = numeric()        	#
gbl.cust.pairs = vector()         	# Output of stage 2; each element is a vector of 
				                	# 2 customer serials who are correlated

obs.period = 3                    	# number of years to observe for deviation
gTimeFreq  = 12             

# >>> CYCLE1:1
# Instructions: Change the sequence to suit the range of customers being processed. Select the
#               1-base sequence, filenames is 0 based. If the last file is X, the last seq item is x+1
seqCustRange = seq(1:7)		        
                                  
MAX_RHO_LIMIT = 0.8               	# Limit for non-stationarity

# >>> CYCLE0:1
# Instructions: Change to an HDFS or a local directory path where data-files are located.
hdfs.data.dir = "/home/hadoop/datafiles"
hdfs.output.dir = "/home/hadoop/output"

#hdfs.data.dir = file.path("C:/Users/mukulbiswas/Documents/My Projects/Elance/7. R-Predictive-Yosef/test_data")
#hdfs.output.dir = file.path("C:/Users/mukulbiswas/Documents/My Projects/Elance/7. R-Predictive-Yosef/output")

# HDFS Example - gbl.data.path = file.path("/user/hadoop/myprojdata")
#gbl.data.path = file.path(paste(gbl.base.path, gbl.data.dir, sep="/"))

#setwd(file.path("C:/Users/mukulbiswas/Documents/My Projects/Elance/7. R-Predictive-Yosef", "src_mapreduce v2.0"))
source("MR_Custom_Functions.R")

# >>> CYCLE0:2
# Instructions: Currently set to local mode of execute. Uncomment this line when ready to
#               run on Hadoop.
#rmr.options(backend = "local")
rmr.options(dfs.tempdir = "/home/hadoop/tempdir") 

#-STEP 1 : Time-series preparation for all the customers data and store in DFS-------------
#------------------------------------------------------------------------------------------


# inputFileVector = vector()
# for(x in 1:7){
#   inputFileVector = c(inputFileVector, paste(x-1,'.csv',sep=''))
# }
# setwd(hdfs.data.dir)

# MapReduce:
#   Map function creates the keys out of the customer number, and values from the dates & amounts
#   Reduce function prepares the time-series and save in dfs.
time.series = mapreduce(
  input         =  hdfs.data.dir, #inputFileVector, #
  input.format  =  make.input.format("csv", sep = ",", col.names=c("CustNo", "Date", "Amount")),
  output.format =  make.output.format("csv", sep = ","),
  
  map = function(k,v){
    key = v$CustNo
    val = data.frame(v$Date, v$Amount)
    keyval(key,val)
  },
  
  reduce = function(k,v){
    # Split Month and Year from the date as 2 additional columns
    tmp.dat = as.POSIXct(v[,1])  
    tmp.mon = strftime(tmp.dat, "%m")
    tmp.yea = strftime(tmp.dat, "%Y")
    tmp.amo = v[,2]
    
    # Aggregate data by month
    tmp.agg = aggregate(tmp.amo~tmp.mon+tmp.yea, v, FUN=sum)
    
    # Make time-series with the starting month and year
    num.row   = nrow(tmp.agg)
    
    last.mnth = as.integer(tmp.agg$tmp.mon[num.row])    #last month on the series
    last.year = as.integer(tmp.agg$tmp.yea[num.row])    #last year  on the series
    frstmy    = XPastMonthYear(last.year, last.mnth, obs.period, num.row)
    frst.year = frstmy[1]
    frst.mnth = frstmy[2]
    
    tmp.tse = ts(tmp.agg$tmp.amo, start=c(as.integer(tmp.agg$tmp.yea[1]), as.integer(tmp.agg$tmp.mon[1])), frequency=gTimeFreq)
    
    # Take the last few years (defined by obs.period) as the subset of data to determine the deviation
    ts.recent.years = window(tmp.tse, start=c(frst.year, frst.mnth), end=c(last.year, last.mnth))
    
    newkey = data.frame(k, frst.year, frst.mnth, last.year, last.mnth)
    keyval (newkey, ts.recent.years)
  }   
)

#-STEP 2 : Trend determination of the time series------------------------------------------
#------------------------------------------------------------------------------------------
# MapReduce:
#   Map prepared the key and value
#   Reduce invokes yoDeviation for all the customers time-series
deviants = mapreduce(
  input         = time.series, 
  input.format  = make.input.format("csv", sep = ",", col.names=c("CustNo", "fy", "fm", "ly", "lm", "Amount")),
  output        = file.path(hdfs.output.dir, "deviants"),
  output.format = make.output.format("csv", sep=","),
  
  map = function(k, v){
    k = data.frame(v$CustNo, v$fy, v$fm, v$ly, v$lm)
    keyval(k, v$Amount)
  },

  reduce = function(k, v){
    vResult = yoDeviation(v, k$v.fy, k$v.fm, k$v.ly, k$v.lm)
    if(vResult[1] == TRUE)
      keyval(k$v.CustNo, vResult[2]) # [1] T/F [2] $pp.value
  }
)

#-STEP 3 : Pairing of the deviant customers (for the next step)----------------------------
#------------------------------------------------------------------------------------------
# Input : Data frame ( Customer ID, List(isDeviant, DeviationCoefficient))
# Output: Listed pair of deviant customers (1-1, 1-2, 1-3, .. 2-1, 2-2, ..)
locDev    = from.dfs(deviants, format="csv")
val       = values(locDev)
valV1     = val$V1
size      = length(valV1)
customers = numeric(size)

for(i in 1:size){
  tmpkey = unlist(strsplit( as.character(valV1[[i]]),","))[[1]]
  customers[i] = as.numeric(tmpkey)
} 

# Initialize a matrix to a size to hold all the pairs
nmatrix = (size^2 - size)/2
customerPairs = matrix(nrow=nmatrix, ncol=2)
counter = 1

for(i in customers){
  for(j in customers){
    if(i < j){
      customerPairs[counter,1] = i
      customerPairs[counter,2] = j
      counter = counter + 1
    }
  }
}

#-STEP 4 : Grouping Customer with similar patterns (Correlation)---------------------------
#------------------------------------------------------------------------------------------
dCustomerPairs = to.dfs(data.frame(customerPairs))

# MapReduce:
#   Map prepared the key and value
#   Reduce invokes yoDeviation for all the customers time-series
correlatedPairs = mapreduce(
    input  = dCustomerPairs, 
    #input.format  = make.input.format("csv", sep = ",", col.names=c("cust1", "cust2")),
    output = file.path(hdfs.output.dir, 'correlated'),
    output.format = make.output.format("csv", sep=","),
    
    # Map function applies YoDeviation function. The output is stored in deviation matrix
    map = function(k, v){
        keyval(v, "rho")        # rho is just a dummy value
    },
    reduce = function(k, v) {
        ts1   = getSalesDataByCustomerIdFromDfs(as.numeric(k[1]))     #ts1 and ts2 are local objects, not dfs objects
        ts2   = getSalesDataByCustomerIdFromDfs(as.numeric(k[2]))
        if(length(ts1) == length(ts2)){
            rho   = cor(ts1, ts2)    #RHO is the correlation coefficient
        } else {
            rho = 0
        }                       
        if(rho >= MAX_RHO_LIMIT) keyval(k, rho) 
    }
)


# STEP 5 : Prediction of sales behavior (AUTO.ARIMA)---------------------------------------
#------------------------------------------------------------------------------------------
# MapReduce:
#   Map reads the time-series data for each deviant customers
#   Reduce perform prediction (calling yoPrediction) for next N months/days
pred.cust.list = mapreduce(
    input = deviants,
    input.format = make.input.format("csv", sep=",", col.names=c("CustNo", "DevCoeff")),
    output = file.path(hdfs.output.dir, 'prediction'),
    output.format = make.output.format("csv", sep=","),
    
    map = function(k, v){
      k =  v$CustNo
      v =  getSalesDataByCustomerIdFromDfs(as.numeric(k))
      keyval(k, v)
    },
    
    reduce = function(k, v){
      result = yoPrediction(v)
      keyval(k, v)
    }
  )