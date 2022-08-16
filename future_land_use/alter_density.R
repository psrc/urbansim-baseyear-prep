library(data.table)
library(RMySQL)

setwd("~/psrc/urbansim-baseyear-prep/future_land_use")

date <- "2022-08-10"

dir <- "." # directory where the input files are. Used for output as well.

# input files
flu.file.name <- paste0("flu_imputed_ptid_", date, ".csv")
parcels.file.name <- paste0("prcls_ptid_v2_", date, ".csv")
constraints.file.name <- paste0("devconstr_v2_", date, ".csv")

# output files
parcels.out.file.name <- paste0("prcls_ptid_v2_upzoned_", date, ".csv")
constraints.out.file.name <- paste0("devconstr_v2_upzoned_", date, ".csv")


# read FLU file
flu <- fread(file.path(dir, flu.file.name))

# identify records to clone
flu[, clone := FALSE]
flu[(MaxFAR_Comm > 0 & MaxFAR_Comm < 1) | (MaxFAR_Office > 0 & MaxFAR_Office < 1) | 
        (MaxFAR_Indust > 0 & MaxFAR_Indust < 1) |  (MaxFAR_Mixed > 0 & MaxFAR_Mixed < 1) | 
        (MaxDU_Res > 0 & MaxDU_Res < 24), clone := TRUE]

# read constraints table
constr <- fread(file.path(dir, constraints.file.name))

# create a subset for cloning
constr.clone <- constr[plan_type_id %in% flu[clone == TRUE, plan_type_id]]

# set new plan_type_id and constraint_id
constr.clone[, `:=`(plan_type_id = plan_type_id + 2000, 
                    development_constraint_id = seq_len(nrow(constr.clone)) + constr[, max(development_constraint_id)])]

# modify the maximum, i.e. here the upzoning happens
constr.clone[constraint_type == "units_per_acre" & maximum > 0 & maximum < 24, maximum := 24]
constr.clone[constraint_type == "far" & maximum > 0 & maximum < 1, maximum := 1]

# read parcels
pcl <- fread(file.path(dir, parcels.file.name))
setnames(pcl, "PIN", "parcel_id")

# if the pcl object contains the column tod_id, comment the block below
# which pulls it from mysql

###################################
### the block below is only needed temporarily until the file above contains tod_id
###################################
# temporary fix
pcl[plan_type_id > 50000, plan_type_id := plan_type_id - 41000]

# read parcels from DB to get tod_id
# the code below could be replaced by reading 
base.db <- "2018_parcel_baseyear"

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
pcl <- merge(pcl, pcltod, by = "parcel_id")

# End of the temporary block. Now pcl should have a column tod_id
################################### 

# select HCT parcels and set an alternative PTID
hctpcl <- pcl[tod_id > 0 & tod_id < 6]
hctpcl[, hct_plan_type_id := plan_type_id + 2000]

# parcels where hct_plan_type_id exists in constr.clone are going to get updated maximum, therefore updated PTID
upzone.pcl <- hctpcl[hct_plan_type_id %in% constr.clone[, plan_type_id]]

# update plan_type_id for the parcels in upzone.pcl and export
upd.pcl <- copy(pcl)
upd.pcl[upzone.pcl, plan_type_id := i.hct_plan_type_id, on = .(parcel_id)]
fwrite(upd.pcl, file = file.path(dir, parcels.out.file.name))

# join the constraints and export
all.constr <- rbind(constr, constr.clone)
fwrite(all.constr, file = file.path(dir, constraints.out.file.name))

# output some info
check.pcl <- merge(upd.pcl, pcl, by = "parcel_id", all = TRUE)
cat("\nplan_type_id updated for ", check.pcl[, sum(plan_type_id.x != plan_type_id.y)], "parcels. Stored in", 
    file.path(dir, parcels.out.file.name))

cat("\n", nrow(constr.clone), "constraints added. Now", nrow(all.constr), "constraints in total. Stored in",
    file.path(dir, constraints.out.file.name))

