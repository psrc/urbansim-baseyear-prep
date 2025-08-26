library(DBI)
library(RMySQL)

# function for establishing mysql connection
mysql.connection <- function(dbname = "2023_parcel_baseyear") {
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

