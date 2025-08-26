library(data.table)

data.dir <- "data" # which directory is the data in
save.into.mysql <- TRUE
save.as.csv <- TRUE

if(save.into.mysql) {
    source("mysql_connection.R")
    db <- "2023_parcel_baseyear_scenario_county_ugb"
    connection <- mysql.connection(db)
}

# load control id tables
subregs <- fread(file.path(data.dir, "control_hcts.csv"))
subregs[, control_id := control_hct_id]
subregs[control_id > 1000, control_id := control_id - 1000]

controls <- fread(file.path(data.dir, "controls.csv"))

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
        dbWriteTable(connection, paste0("annual_", what, "_control_totals"), 
                     new.ct, overwrite = TRUE, row.names = FALSE)
    if(save.as.csv) 
        fwrite(new.ct, paste0("annual_", what, "_control_totals_new", ".csv"))
}

if(save.into.mysql){
    # save crosswalk
    dbWriteTable(connection, "crosswalk_control_hct_subreg_id", 
                 crosswalk[, .(control_hct_id, subreg_id)], overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "subregs", 
                 new.subregs[order(subreg_id)], overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}