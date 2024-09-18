# Add lockouts to parcels table, based on specific parcel_id

library(data.table)

load.from.mysql <- FALSE
save.in.mysql <- TRUE
db <- "psrc_2023_parcel_baseyear"

lockout.file <- "parcel_lockouts.csv"

lock.id <- 9999

data.dir <- "dev_constraints"
parcels.inname <- "prcls_ptid_v2_2024-09-17" # contains parcel_id, plan_type_id
parcels.outname <- "prcls_ptid_v3_20240918" # name of the resulting dataset

parcels.to.lock <- fread(lockout.file)

if(load.from.mysql || save.in.mysql) source("mysql_connection.R")

if(load.from.mysql){
    connection <- mysql.connection(db)
    qr <- dbSendQuery(db, paste0("select * from ", parcels.inname))
    plan.types <- data.table(fetch(qr, n = -1))
    dbClearResult(qr)
    DBI::dbDisconnect(connection)
} else plan.types <- fread(file.path(data.dir, paste0(parcels.inname, ".csv")))

plan.types[PARCEL_ID %in%parcels.to.lock$parcel_id & plan_type_id < 9000, plan_type_id := lock.id]


if(save.in.mysql){
    connection <- mysql.connection(db)
    dbWriteTable(connection, parcels.outname, plan.types, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
} else fwrite(plan.types, file = file.path(data.dir, paste0(parcels.outname, ".csv")))
