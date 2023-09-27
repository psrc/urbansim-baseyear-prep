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
                  Pierce = c("txt", "|"),
                  Snohomish = c("txt", ",")
                )
overwrite.existing.tables <- FALSE # if FALSE it only exports tables not previously exported 
chunk.size <- 500000 # number of rows to export at once

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
        last.row <- min(chunk.size, nrow(dat)) # index of the last row to export
        cat(" ... rows:", nrow(dat))
        if(nrow(dat) > last.row)
          cat(" (total); exporting ", last.row)
        dbWriteTable(connection, short.name, dat[1:last.row], overwrite = overwrite.existing.tables, 
                        row.names = FALSE)
        first.row <- last.row + 1
        last.row <- first.row + chunk.size - 1
        while(first.row <= nrow(dat)) {
          # we only get here if the dataset is exported in multiple chunks
            cat(", ", last.row)
            dbWriteTable(connection, short.name, 
                         dat[first.row:min(nrow(dat), last.row)], overwrite = FALSE, 
                         append = TRUE, row.names = FALSE)
            first.row <- last.row + 1
            last.row <- first.row + chunk.size - 1
        }
      } else cat(" ... skipped")
    } # all files processed
    # close DB connection
    DBI::dbDisconnect(connection)
} # all counties processed

cat("\nExport finished.\n")

