library(data.table)
library(RMySQL)

# set your working directory
setwd("~/psrc/urbansim-baseyear-prep/future_land_use")

date <- "2022-08-16" # date the input files were created

dir <- "." # directory where the input files are. Used for output as well.

# upzoning parameters
upzone.by.scaling <- TRUE # if FALSE, the upzoning is done by setting values to max(value, max.density.const) 
scaling.factor <- 2
max.density.scaling <- list(far = 10, du = 400) # scale everything smaller than this
max.density.const <- list(far = 1, du = 24)
max.density <- if(upzone.by.scaling) max.density.scaling else max.density.const

# input files
flu.file.name <- paste0("flu_imputed_ptid_", date, ".csv")
parcels.file.name <- paste0("prcls_ptid_v2_", date, ".csv")
constraints.file.name <- paste0("devconstr_v2_", date, ".csv")
pull.tod.from.db <- TRUE # should tod_id be pulled from mysql; FALSE if the parcels.file.name already contains tod_id
base.db <- "2018_parcel_baseyear" # only used if pull.tod.from.db is TRUE

# output files
file.name.part <- if(upzone.by.scaling) paste0("scale", scaling.factor, "_") else ""
parcels.out.file.name <- paste0("prcls_ptid_v2_upzoned_", file.name.part, date, ".csv")
constraints.out.file.name <- paste0("devconstr_v2_upzoned_", file.name.part, date, ".csv")


# read FLU file
flu <- fread(file.path(dir, flu.file.name))

# identify FLU records to clone
flu[, clone := FALSE]

flu[(MaxFAR_Comm > 0 & MaxFAR_Comm < max.density$far) | (MaxFAR_Office > 0 & MaxFAR_Office < max.density$far) | 
        (MaxFAR_Indust > 0 & MaxFAR_Indust < max.density$far) |  (MaxFAR_Mixed > 0 & MaxFAR_Mixed < max.density$far) | 
        (MaxDU_Res > 0 & MaxDU_Res < max.density$du), clone := TRUE]

# read constraints table
constr <- fread(file.path(dir, constraints.file.name))

# create a subset for cloning that corresponds to the selected FLU records
constr.clone <- constr[plan_type_id %in% flu[clone == TRUE, plan_type_id]]

# set new plan_type_id and constraint_id for these cloned records
constr.clone[, `:=`(plan_type_id = plan_type_id + 2000, 
                    development_constraint_id = seq_len(nrow(constr.clone)) + constr[, max(development_constraint_id)])]

# modify the maximum, i.e. here the upzoning happens
if(upzone.by.scaling){
    constr.clone[constraint_type == "units_per_acre" & maximum > 0 & maximum < max.density$du, maximum := maximum * scaling.factor]
    constr.clone[constraint_type == "far" & maximum > 0 & maximum < max.density$far, maximum := maximum * scaling.factor]
} else {
    constr.clone[constraint_type == "units_per_acre" & maximum > 0 & maximum < max.density$du, maximum := max.density$du]
    constr.clone[constraint_type == "far" & maximum > 0 & maximum < max.density$far, maximum := max.density$far]
}

# read parcels
pcl <- fread(file.path(dir, parcels.file.name))
setnames(pcl, "PIN", "parcel_id")


if(pull.tod.from.db){ 
    # the tod_id attribute will be pulled from mysql,
    # otherwise the parcel file is expected to contain tod_id
    pcl[plan_type_id > 50000, plan_type_id := plan_type_id - 41000]

    # read parcels from DB to get tod_id
    
    # Connecting to Mysql
    mysql.connection <- function(dbname = "2018_parcel_baseyear") {
        # credentials can be stored in a file (as one column: username, password, host)
        if(file.exists("creds.txt")) {
            creds <- read.table("creds.txt", stringsAsFactors = FALSE)
            un <- creds[1,1]
            psswd <- creds[2,1]
            if(nrow(creds) > 2) h <- creds[3,1]
            else h <- .rs.askForPassword("host:")
        } else {
            un <- .rs.askForPassword("username:")
            psswd <- .rs.askForPassword("password:")
            h <- .rs.askForPassword("host:")
        }
        dbConnect(MySQL(), user = un, password = psswd, dbname = dbname, host = h)
    }
    
    mydb <- mysql.connection(base.db)
    qr <- dbSendQuery(mydb, paste0("select parcel_id, tod_id from parcels"))
    pcltod <- data.table(fetch(qr, n = -1))
    dbClearResult(qr)
    
    # merge with the other parcel dataset
    if("tod_id" %in% colnames(pcl))
        pcl[, tod_id := NULL]
    pcl <- merge(pcl, pcltod, by = "parcel_id")
}

# Now pcl should have a column tod_id
###################################

# select HCT parcels and set an alternative (theoretical) PTID
hctpcl <- pcl[tod_id > 0 & tod_id < 6]
hctpcl[, hct_plan_type_id := plan_type_id + 2000]

# select parcels where the theoretical hct_plan_type_id exists in constr.clone
upzone.pcl <- hctpcl[hct_plan_type_id %in% constr.clone[, plan_type_id]]

# update plan_type_id for the parcels in upzone.pcl and export
upd.pcl <- copy(pcl)
upd.pcl[upzone.pcl, plan_type_id := i.hct_plan_type_id, on = .(parcel_id)]
fwrite(upd.pcl, file = file.path(dir, parcels.out.file.name))

# remove unused constraints, join with the original set and export
new.constr <- constr.clone[plan_type_id %in% upd.pcl[, plan_type_id]]
all.constr <- rbind(constr, new.constr)
fwrite(all.constr, file = file.path(dir, constraints.out.file.name))

# output some info
check.pcl <- merge(upd.pcl, pcl, by = "parcel_id", all = TRUE)
cat("\nplan_type_id updated for ", check.pcl[, sum(plan_type_id.x != plan_type_id.y)], "parcels. Stored in", 
    file.path(dir, parcels.out.file.name))

cat("\n", nrow(new.constr), "constraints added. Now", nrow(all.constr), "constraints in total. Stored in",
    file.path(dir, constraints.out.file.name))

cat("\nIt corresponds to", length(new.constr[, unique(plan_type_id)]), "new plan types.")

cat("\n")
