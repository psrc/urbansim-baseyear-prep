# Script for aggregating control totals from control_hcts into 
# a cross-section of county and UGB.
# 
# Hana Sevcikova, 08/26/2025

library(data.table)

# which directory is the data in
data.dir <- "data" 

# Should the new CTs be written into mysql and csv files? 
# If use.existing.crosswalk is FALSE, then the resulting crosswalk would 
# be also written.
save.into.mysql <- FALSE
save.as.csv <- TRUE

# Read input data from mysql. These are tables:
# if use.existing.crosswalk is FALSE: control_hcts, controls
# if use.existing.crosswalk is TRUE: crosswalk_control_hct_subreg_id
# Otherwise these tables are read from csv files in the data.dir directory
read.from.mysql <- FALSE

# Should the crosswalk be newly created (FALSE)
# or has it been created before (TRUE)
use.existing.crosswalk <- FALSE

# name of the crosswalk table (used for input and output)
crosswalk.name <- "crosswalk_control_hct_subreg_id"

# If mysql is involved, which databases should be used 
# to read data (in) and write results (out)
db.in <- "2023_parcel_baseyear_scenario_county_ugb"
db.out <- "2023_parcel_baseyear_scenario_county_ugb"

# establish mysql connections
if(read.from.mysql || save.into.mysql) {
    source("mysql_connection.R")
    if(save.into.mysql)
      connection.out <- mysql.connection(db.out)
    if(read.from.mysql)
      connection.in <- mysql.connection(db.in)
}

if(!use.existing.crosswalk) {
  # create a new crosswalk
  # load control id tables
  if(read.from.mysql) {
    qr <- dbSendQuery(connection.in, "select * from control_hcts")
    subregs <- data.table(fetch(qr, n = -1))
    dbClearResult(qr)
    qr <- dbSendQuery(connection.in, "select * from controls")
    controls <- data.table(fetch(qr, n = -1))
    dbClearResult(qr)
  } else {
    subregs <- fread(file.path(data.dir, "control_hcts.csv"))
    controls <- fread(file.path(data.dir, "controls.csv"))
  }
  subregs[, control_id := control_hct_id]
  subregs[control_id > 1000, control_id := control_id - 1000]
  subregs[controls, is_ugb := as.integer(i.control_rgs_id <= 5), on = "control_id"]
  counties <- fread(file.path(data.dir, "counties.csv"))

  # construct new subregs from a combination of is_ugb and county
  new.subregs <- subregs[, .N, by = c("county_id", "is_ugb")]
  new.subregs[, subreg_id := 1:nrow(new.subregs)]
  new.subregs <- merge(new.subregs, counties)
  new.subregs <- merge(new.subregs, data.table(is_ugb = c(0, 1), ugb_name = c("out of UGB", "in UGB")), by = "is_ugb")
  new.subregs[, name := paste(county_name, ugb_name)][, `:=`(county_name = NULL, ugb_name = NULL, N = NULL)]

  # crosswalk between old and new subreg_id
  crosswalk <- merge(subregs, new.subregs, by = c("county_id", "is_ugb"))
} else {
  # use existing crosswalk
  if(read.from.mysql) {
    qr <- dbSendQuery(connection.in, paste("select * from", crosswalk.name))
    crosswalk <- data.table(fetch(qr, n = -1))
    dbClearResult(qr)
  } else crosswalk <- fread(file.path(data.dir, paste0(crosswalk.name, ".csv")))
}
ct.settings <- list(household = "total_number_of_households", employment = "total_number_of_jobs")

# iterate over HHs and jobs CTs
for(what in names(ct.settings)){
    # load the CT file
    ct <- fread(file.path(data.dir, paste0("annual_", what, "_control_totals.csv")))

    # subset rows with given subreg_id
    ct.loc <- ct[subreg_id > -1]

    # assign new subreg_id
    setnames(ct.loc, "subreg_id", "old_subreg_id")
    ct.loc[crosswalk, subreg_id := i.subreg_id, on = c(old_subreg_id = "control_hct_id")]

    # aggregate over new ids
    new.ct.loc <- ct.loc[, .(total = sum(as.integer(get(ct.settings[[what]])))), 
                     by = setdiff(colnames(ct.loc), c(ct.settings[[what]], "old_subreg_id"))]
    setnames(new.ct.loc, "total", ct.settings[[what]])

    # join with regional CTs
    new.ct <- rbind(ct[ subreg_id == -1], new.ct.loc)[order(year)]
    
    # export
    if(save.into.mysql) 
        dbWriteTable(connection.out, paste0("annual_", what, "_control_totals"), 
                     new.ct, overwrite = TRUE, row.names = FALSE)
    if(save.as.csv) 
        fwrite(new.ct, paste0("annual_", what, "_control_totals_new", ".csv"))
}

if(save.into.mysql){
  if(!use.existing.crosswalk){
      # save crosswalk
      dbWriteTable(connection.out, crosswalk.name, 
                 crosswalk[, .(control_hct_id, subreg_id)], overwrite = TRUE, row.names = FALSE)
      dbWriteTable(connection.out, "subregs", 
                 new.subregs[order(subreg_id)], overwrite = TRUE, row.names = FALSE)
  }
  DBI::dbDisconnect(connection.out)
}

if(read.from.mysql) DBI::dbDisconnect(connection.in)

if(save.as.csv && !use.existing.crosswalk) 
  fwrite(crosswalk, paste0(crosswalk.name, ".csv"))
  