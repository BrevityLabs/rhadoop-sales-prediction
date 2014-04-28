#! /usr/bin/env Rscript

# Source name : src_mapreduce/MR_Custom_Functions.R
# Project Name: Behavior Pattern Recognition (Elance)
# Client name : Yosef
# Author      : mukul.biswas.ie@ieee.org
# version     : Initial submission
#-----------------------------------------------------------------------------------------
# Purpose     : The program reads a number of customers-wise sales data and converts
#               This module contains the functions used in the process of behavior
#               Pattern Recognition. These functions are MapReduce aware and work on a
#               distributed environment in their current form. The list of functions are -
#               1. yoDeviation - Accepts a time-series and determines its non-stationarity
#               2. yoCorrelation - Determines correlation between a given pair of time-
#                   series
#               3. yoPrediction - Predicts occurrance of sales in future N time intervals
#               4. XPastMonthYear - A utility function returns month/year a certain period
#                   in the past
#-----------------------------------------------------------------------------------------


# DETERMINE DEVIATIONS OF A TIME-SERIES ---------------------------------------------------
# The function accepts the serial number of the file to be processed, creates a time-series
# and performs test for stationarity (opposite of deviating). If deviating then return the
# serial number, and the factor by with the deviation took place over last 2 years.
#
#	Input	:	Input file denoted by the serial number
#	Output	:   A vector with 2 elements
#               1. the serial number
#               2. deviation factor (1 - no deviation; > 1- increasing; < 1 - decreasing)
#
#------------------------------------------------------------------------------------------
yoDeviation = function(vInput, fy, fm, ly, lm){
	
  tryOk = TRUE    							# Just initialize the flag once, if fails method returns anyway
  errcode = 0
  tryOk = tryCatch( {
      silent = TRUE
      ts.data = ts(vInput, start=c(fy, fm), frequency=gTimeFreq)
      series.components = decompose(ts.data)	# There will be preceding and trailing NA values; 6 of them each
      errcode = 1
      
      #Rest of the steps is to clear the NA value because they interfere in PP.Test. We know that NA occurs in
      #half the frequency leading and trailing data points
    	halfTheFreq = gTimeFreq/2
      
      if(fm + halfTheFreq > 12) {
        fm1 = fm + halfTheFreq - 12
        fy1 = fy + 1
      } else {
        fm1 = fm + halfTheFreq
        fy1 = fy
      }
    	
    	if(lm - halfTheFreq < 1) {
    	  lm1 = 12 + lm - halfTheFreq
        ly1 = ly - 1
    	} else {
    	  lm1 = lm - halfTheFreq
        ly1 = ly
    	}
      
      final_trend = window(series.components$trend, start=c(fy1, fm1), end=c(ly1, lm1))
      errcode = 2
      
	    # Check deviation parameters, see if the trend is not stationary using PP.test and Box.test
      pp.result = PP.test(final_trend)
      errcode = 3
  
      isDeviating = FALSE              #Initializing
    	
      if(pp.result$p.value > 0.05){
    			isDeviating = TRUE
      	} else {
          box.result = Box.test(final_trend, lag=20, type="Ljung-Box")
          errcode = 4
    		  if(box.result$p.value > 0.05) isDeviating = TRUE
    	}
    },
    error = function(e) {
      if(errcode == 0) print("Error: in decomposition")
      if(errcode == 1) print("Error: in window method")
      if(errcode == 2) print("Error: in PP.Test")
      if(errcode == 3) print("Error: in Box.test")
      if(errcode == 4) print("Unknown Error")
      return(FALSE)
    }	)
    
    if(tryOk == FALSE) return(list(FALSE, 0))
  
  
	
	# If the trend is deviating then determine the rate of ascend or descend
	if(isDeviating){
	  this.year.mean = mean(window(final_trend, start=c(ly-1, fm), end=c(ly,  lm)))
		last.year.mean = mean(window(final_trend, start=c(fy-2, fm), end=c(ly-1, lm)))
		
		change.fact = this.year.mean/last.year.mean		                    # signifies +ve or -ve swing
		rm(isDeviating, final_trend, series.components)		                # no sure if there is a GC in R
		return(list(TRUE, change.fact))
  } 
} #end function


# DETERMINATION of CORRELATION BETWEEN 2 TIME-SERIES---------------------------------------
# Function finds correlation between 2 series
# Arguments: The series by deviant customer serial numbers (0,1,2, ...)
# Returns  : A vector with series 1 serial, series 2 serial, correlation coefficient if 
#            relevant return with 0,0,0 values means ignore.
#------------------------------------------------------------------------------------------
yoCorrelation = function(ts.i, ts.j) {
  corcoff = cor( ts.i, ts.j, use="pairwise.complete.obs", method="kendall")
  if(corcoff > MAX_RHO_LIMIT) return(corcoff) else return(NULL)
}


# PREDICTION OF FUTURE OCCURRENCES OF SALES -----------------------------------------------
# The function accepts the serial number of the file to be processed, creates a time-series
# and performs test for stationarity (opposite of deviating). If deviating then return the
# serial number, and the factor by with the
#
# Arguments	:	The time-series data
# Returns	:	A vector with 2 elements
#				1. the serial number
#				2. deviation factor (1 - no deviation; > 1- increasing; < 1 - decreasing)
#
#------------------------------------------------------------------------------------------
yoPrediction = function(tsdata, numFc = 6){
  library(forecast)
  
  tryOk = TRUE
  tryOk = tryCatch( {
     fit = auto.arima(tsdata) 				# arima(amt.agg.ts.lim, c=(0,2,2))
    
    fore.fit = forecast(fit, numFc)
    silent = TRUE
  },
  error = function(e) {
    print("Error: in Auto Arima")
    return(FALSE)
  }
  )# end of tryCatch
  
  
  if(tryOk == FALSE) {
    foo = numeric(length = numFc)
    return(c(i, foo))
  } else {
    return(c(i, fore.fit$mean))
  }
}

# Function 3: getSalesDataByCustomerIdFromDfs
#       Extracts one time-series from the data-frame containing all the time-series -
# Arguments:    customer ID
# Returns:      Time-series for a given customer ID
# This function should be called only after the main program as once created the time.series
# data-frame object
#------------------------------------------------------------------------------------------
getSalesDataByCustomerIdFromDfs = function(cust.id){
    tmpVector = numeric()
    one.time.series = mapreduce(
        input         =  time.series,
        input.format  =  make.input.format("csv", sep = ",", col.names=c("CustNo", "fy", "fm", "ly", "lm", "Amount")),
        #output        =  file.path("one_time_series.txt"),
        output.format =  make.output.format("csv", sep = ","),
        
        map = function(k,v) keyval(v$CustNo, v$Amount),
        reduce = function(k, v) if(as.numeric(k) == cust.id) keyval(k, v)
     )
    # bringing timeseries to the local memory. it should not hamper performance because each timeseries
    # itself is not big.
    tmpList  = from.dfs(one.time.series, format="csv")    # type: text; format: {NULL} {key,value}
    tmpKeyVals = tmpList$val$V1                             # type: text; format: {key,value}
    for(item in tmpKeyVals) {
        tmpVector = c(tmpVector, as.numeric(strsplit(as.character(item),",")[[1]][2]))
    }
    return(tmpVector)
}

# DETERMINES MONTH-YEAR IN THE PATH -------------------------------------------------------
# Arguments:    The reference month and year, period lapsed, number of observations
# Returns:      A vector of resultant month and year
# 
# Note: obs.period is in years
#------------------------------------------------------------------------------------------

XPastMonthYear = function(last.year, last.mnth, obs.period=3, num.obs=36){
  frst.mnth = 0
  frst.year = 0
  if(num.obs >= obs.period*12){
    if(last.mnth == 12) {
      frst.mnth = 1
      frst.year = last.year - obs.period + 1
    } else {
      frst.mnth = last.mnth + 1
      frst.year = last.year - obs.period
    }
  } else {
    m = num.obs %%  12
    y = num.obs %/% 12
    frst.mnth = last.mnth - m + 1
    frst.year = last.year - y
    if(frst.mnth == 0){
      frst.mnth = 12
      frst.year = frst.year - 1 
    }
    if(frst.mnth == 13){
      frst.mnth = 1
      frst.year = frst.year + 1 
    }
    
  }
  
  return(c(frst.year,frst.mnth))
}
