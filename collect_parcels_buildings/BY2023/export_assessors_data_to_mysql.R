# Script for exporting Assessor's text files into MYSQL
#
# Hana Sevcikova, 09/25/2023
#

library(data.table)
library(DBI)
library(RMySQL)

# define file endings & column separators for each county
file.info <- list(King = c("csv", ","),    
                  Kitsap = c("txt", "\t"),
                  Pierce = c("csv", ","),
                  Snohomish = c("csv", ",")
                )
overwrite.existing.tables <- FALSE # if FALSE it only exports tables not previously exported 

# which counties should be processed
county.dirs <- rev(names(file.info)) # all; rev function makes it run in counties' reverse order, i.e. King is last

# function for establishing mysql connection
mysql.connection <- function(dbname = "2018_parcel_baseyear") {
  # credentials can be stored in a file (as one column: username, password, host)
  if(file.exists(".creds.txt")) {
    creds <- read.table(".creds.txt", stringsAsFactors = FALSE)
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

# Start main processing
#######################

for(county in county.dirs){ # iterate over county directories
    # identify files to be exported, i.e get names of all files in the county directory 
    # with the given suffix
    files <- list.files(county, pattern = paste0("*.", file.info[[county]][1])) 
    if(length(files) == 0) next # no files; skip to the next county
    
    cat("\nExporting", length(files), "datasets in", county)
    
    # connect to the database
    db <- paste0(tolower(county), "_2023_parcel_baseyear")
    connection <- mysql.connection(db)
    
    # iterate over files
    for(f in files){
      # remove suffix from the name to get mysql table name
      short.name <- tolower(strsplit(f, paste0(".", file.info[[county]][1]))[[1]]) 
      cat("\n\t\t", short.name)
      
      # load data and store into MySQL if needed
      if(overwrite.existing.tables || !dbExistsTable(connection, short.name)){
        # load data
        dat <- fread(file.path(county, f), sep = file.info[[county]][2])
        # store into MySQL 
        dbWriteTable(connection, short.name, dat, overwrite = overwrite.existing.tables, 
                        row.names = FALSE) 
      } else cat(" ... skipped")
    } # all files processed
    # close DB connection
    DBI::dbDisconnect(connection)
} # all counties processed

cat("\nExport finished.\n")

