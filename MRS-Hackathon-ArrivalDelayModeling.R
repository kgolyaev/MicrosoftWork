rm(list = ls())
set.seed(1)
setwd("D:/rHackathon/")
# we need packages, so let's install them
packages <- c("dplyr", "ggplot2", "stringr", "tidyr", "rmarkdown", "knitr",
              "assertr", "glmnet", "devtools")
for (package in packages) {
  if (!require(package, character.only = TRUE)) {
    install.packages(package, character.only = TRUE)
    library(package, character.only = TRUE)
  }
}
library("RevoScaleR")
# dplyrXdf is on github, using devtools to install
devtools::install_github("Hong-Revo/dplyrXdf")
library("dplyrXdf")

# I created a lookup table for airline names, ended up not using it
airlineNameTable <- data.frame(
  airlineCode = c("PS", "TW", "UA", "WN", "EA", "HP", "NW", "PA (1)", "PI",
                    "CO", "DL", "AA", "US", "AS", "ML (1)", "AQ", "MQ", "OO",
                    "XE", "TZ", "EV", "FL", "B6", "DH", "HA", "OH", "F9", "YV",
                    "9E"),
  airlineName = c("Ukraine International Airlines",
                  "Trans World Airways LLC",
                  "United Air Lines Inc.",
                  "Southwest Airlines Co.",
                  "Eastern Air Lines Inc.",
                  "America West Airlines Inc.",
                  "Northwest Airlines Inc.",
                  "Pan American World Airways",
                  "Piedmont Aviation Inc.",
                  "Continental Air Lines Inc.",
                  "Delta Air Lines Inc.",
                  "American Airlines Inc.",
                  "US Airways Inc.",
                  "Alaska Airlines Inc.",
                  "Midway Airlines Inc.",
                  "Aloha Airlines Inc.",
                  "Envoy Air",
                  "SkyWest Airlines Inc.",
                  "ExpressJet Airlines Inc.",
                  "ATA Airlines d/b/a ATA",
                  "ExpressJet Airlines Inc.",
                  "AirTran Airways Corporation",
                  "JetBlue Airways",
                  "Independence Air",
                  "Hawaiian Airlines Inc.",
                  "PSA Airlines Inc.",
                  "Frontier Airlines Inc.",
                  "Mesa Airlines Inc.",
                  "Endeavor Air Inc."),
  stringsAsFactors = FALSE)

### Most RevoScaleR functions start with rx. The rxGetInfo() function examines the metadata on a dataset
### We cannot use str() since data are stored on disk.

rxGetInfo("AirlineDataSubsample.xdf", getVarInfo = TRUE, numRows = 5)
rxGetInfo("AirlineData87to08.xdf", getVarInfo = TRUE, numRows = 5)

# rxDataStep performs data transformations.
# Here we throw out any flights not originating in airports of interest
rxDataStep(inData = "AirlineData87to08.xdf",
           outFile = "mainData.xdf",
           #outFile = "sample.xdf",
           #rowSelection = (Year == 2008) & (Origin == "SEA"),
           rowSelection = (Origin %in% c("SEA", "BOS", "SJC", "SFO")),
           overwrite = TRUE,
           reportProgress = 1)
# clean up missing values and keep only columns we'll need later
# the last argument makes sure we materialize a large data frame in RAM
df <- rxDataStep(inData = "sample.xdf",
                 rowSelection = !is.na(DepDelay) &
                   !is.na(DepTime) & !is.na(ArrDelay),
                 varsToKeep = c("Year",
                                "Month",
                                "DayofMonth",
                                "DayOfWeek",
                                "Origin",
                                "Dest",
                                "DepTime",
                                "DepDelay",
                                "ArrDelay",
                                "UniqueCarrier"
                                ),
                 maxRowsByCols = Inf)
# xdf <- RxXdfData(file = "sample.xdf",
# another function to push the contents of xdf file into data frame in RAM
xdf <- RxXdfData(file = "mainData.xdf",
                 varsToKeep = c("Year",
                                "Month",
                                "DayofMonth",
                                "DayOfWeek",
                                "Origin",
                                "Dest",
                                "DepTime",
                                "DepDelay",
                                "ArrDelay",
                     "UniqueCarrier"))

#####################################################
### From here onwards the actual analysis happens ###
#####################################################

# data summary by carrier
carrierSummaryXdf <- xdf %>%
  filter(
    !is.na(DepDelay) & !is.na(DepTime) & !is.na(ArrDelay)
  ) %>%
  group_by(UniqueCarrier) %>%
  summarize(nRows = n()) %>%
  mutate(airlineCode = as.character(UniqueCarrier)) %>%
  inner_join(airlineNameTable, by = "airlineCode") %>%
  arrange(desc(nRows))
# this step materializes the temp file from above
rxDataStep(inData = carrierSummaryXdf)

# data summary by destination
destinationSummaryXdf <- xdf %>%
  filter(
    !is.na(DepDelay) & !is.na(DepTime) & !is.na(ArrDelay)
  ) %>%
  group_by(Dest) %>%
  summarize(nRows = n()) %>%
  arrange(desc(nRows))
rxDataStep(inData = destinationSummaryXdf)

# clean data: prune rows with missing values and truncate outliers
xdfClean <- xdf %>%
  filter(
    !is.na(DepDelay) & !is.na(DepTime) & !is.na(ArrDelay) &
      (ArrDelay <= 120) & (ArrDelay >= -120)
  ) %>%
  mutate(
    DepHour = floor(DepTime),
    DelayBin = floor(ArrDelay / 30),
    Origin   = ifelse(Origin %in% c("SJC", "SFO"), "SVC",
                      ifelse(Origin == "SEA", "SEA", "BOS"))
    #DelayBin = ArrDelay
  ) %>%
  group_by(
    Year,
    Origin,
    #Dest,
    #UniqueCarrier,
    DayOfWeek,
    DepHour,
    DelayBin
  ) %>%
  summarize(count = n()) %>%
  arrange(
    Year,
    Origin,
    #Dest,
    #UniqueCarrier,
    DayOfWeek,
    DepHour,
    DelayBin
  )
rxDataStep(inData = xdfClean,
           outFile = "xdfClean.xdf",
           overwrite = TRUE)
rxGetInfo(xdfClean, getVarInfo = T, numRows = 2)

# prepare data for making impromptu histograms
totalsForBinsXdf <- xdfClean %>%
  group_by(
    Year,
    Origin,
    #Dest,
    #UniqueCarrier,
    DayOfWeek,
    DepHour
  ) %>%
  summarize(binCount = sum(count)) %>%
  arrange(
    Year,
    Origin,
    #Dest,
    #UniqueCarrier,
    DayOfWeek,
    DepHour
  )
rxDataStep(inData = totalsForBinsXdf,
           outFile = "totalsForBinsXdf.xdf",
           overwrite = TRUE)
rxGetInfo(totalsForBinsXdf, getVarInfo = T, numRows = 2)

# somehow native MRS function rxMerge() worked better than
# the dplyrXdf::inner_join()
proportionsXdf <- rxMerge(
  inData1 = "xdfClean.xdf",
  inData2 = "totalsForBinsXdf.xdf",
  matchVars = intersect(names(rxDataStep("xdfClean.xdf")),
                        names(rxDataStep("totalsForBinsXdf")))
  ) %>%
  mutate(
    freq = 100 * round(count / binCount, digits = 4),
    Hour = ifelse(DepHour > 12, DepHour - 12, DepHour),
    Hour = ifelse(DepHour < 12 , str_c(Hour, " AM"), str_c(Hour, " PM"))
  ) %>%
  arrange(
    Year,
    Origin,
    #Dest,
    #UniqueCarrier,
    DayOfWeek,
    DepHour,
    DelayBin
  )
rxDataStep(inData = proportionsXdf,
           outFile = "proportionsXdf.xdf",
           overwrite = TRUE)
cleanProportionsXdf <- proportionsXdf %>%
  select(-one_of("Year", "count", "binCount"))
prop <- rxDataStep(inData = cleanProportionsXdf)
View(prop)

# histograms are computed, now bindata by departure time
mainData <- prop %>%
  mutate(Departure = ifelse(DepHour < 6, "12AM to 06AM",
                       ifelse(DepHour < 12, "06AM to 12PM",
                         ifelse(DepHour < 18, "12PM to 06PM", "06PM to 12AM"))),
         Departure = factor(Departure,
                            levels = c("12AM to 06AM", "06AM to 12PM",
                                       "12PM to 06PM", "06PM to 12AM"),
                            ordered = TRUE)
         ) %>%
  group_by(
    Origin,
    Departure,
    DayOfWeek,
    DelayBin
  ) %>%
  summarize(freq = mean(freq)) #%>%
  #filter(DepHour == 0)

# making exhaustive grid for plotting
mainGrid <- expand.grid(
  unique(mainData[["Origin"]]),
  unique(mainData[["Departure"]]),
  unique(mainData[["DayOfWeek"]]),
  unique(mainData[["DelayBin"]])
)
names(mainGrid) <- c("Origin", "Departure", "DayOfWeek", "DelayBin")

forPlot <- merge(mainGrid, mainData, all.x = TRUE,
                 by = c("Origin", "Departure", "DayOfWeek", "DelayBin")) %>%
  mutate(freq = ifelse(is.na(freq), 0, freq))

# using ggplot to produce plots
plotBOS <- ggplot(data = filter(forPlot, Origin == "BOS"),
               aes(x = DelayBin,
                   y = freq)
               ) +
        #facet_grid(DayOfWeek ~ .) +
        facet_grid(Departure ~ DayOfWeek) +
        geom_line() + geom_point() +
        geom_vline(xintercept = 0, color = "blue")
plotSEA <- ggplot(data = filter(forPlot, Origin == "SEA"),
               aes(x = DelayBin,
                   y = freq)
               ) +
        #facet_grid(DayOfWeek ~ .) +
        facet_grid(Departure ~ DayOfWeek) +
        geom_line() + geom_point() +
        geom_vline(xintercept = 0, color = "blue")
plotSVC <- ggplot(data = filter(forPlot, Origin == "SVC"),
               aes(x = DelayBin,
                   y = freq)
               ) +
        #facet_grid(DayOfWeek ~ .) +
        facet_grid(Departure ~ DayOfWeek) +
        geom_line() + geom_point() +
        geom_vline(xintercept = 0, color = "blue")
print(plotBOS)
print(plotSEA)
print(plotSVC)


