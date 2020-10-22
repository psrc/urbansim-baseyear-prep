# Impute development constraints and compare to previous density table
library(stringr)
library(purrr)
library(magrittr)
library(data.table)

# read FLU file
flu <- fread("W:/gis/projects/compplan_zoning/density_table_4_gis.csv")

# missing Mixed
id.cols <- c(str_subset(colnames(flu), "^Juris|Zone"), "Key", "Definition")
use.cols <- str_subset(colnames(flu), "^[R|C|O|I].*_Use$")
max.cols <- c(str_subset(colnames(flu), "^MaxD.*_[R].*"), str_subset(colnames(flu), "^MaxF.*_[C|O|I].*"))
maxht.cols <- str_subset(colnames(flu), "^MaxHt.*_[R|C|O|I].*")

# organize column names
cols.sets <- pmap(list(use.cols, max.cols, maxht.cols), list)  %>% 
  map(~set_names(.x, c("use", "dens", "height"))) %>% 
  set_names(c("Res", "Comm", "Office", "Indust"))

# add mixed use to cols.sets
cols.sets[['Mixed']] <- list(use = "Mixed_Use", 
                             dens = list(du = "MaxDU_Mixed", far = "MaxFAR_Mixed"),
                             height = "MaxHt_Mixed")

# clean data (temp)
for (col in c(maxht.cols, max.cols, cols.sets$Mixed$dens, cols.sets$Mixed$height)) {
  if(is.character(col)) flu[, (col) := as.numeric(str_extract(get(eval(col)), "\\d+"))]
}

# loop through cols.sets, create new '_imp' columns with imputed values if max height is available
for (i in 1:length(cols.sets)) {
  print(names(cols.sets[i]))
  
  for (j in cols.sets[[i]]$dens) {
    print(j)
    
    use.col <-cols.sets[[i]]$use
    ht.col <- cols.sets[[i]]$height
    
    newcolnm <- paste0(j, "_imp")
    
    # calculation, set to new column ending in '_imp'
    ifelse(str_detect(j, "DU"),
           equat <- parse(text = paste0(newcolnm, ":= (", ht.col, "/15)^ 2")),
           equat <- parse(text = paste0(newcolnm, ":=", ht.col, "/20")))
    
    # density columns (switch for Mixed Use)
    if (names(cols.sets[i]) == "Mixed") {
      ifelse(str_detect(j, "DU"), density.col <- cols.sets[[i]]$dens$du, density.col <- cols.sets[[i]]$dens$far)
    } else {
      density.col <- cols.sets[[i]]$dens
    }
    
    # impute if density is na and height available
    flu[get(eval(use.col)) == "Y" & is.na(get(eval(density.col))) & !is.na(get(eval(ht.col))), eval(equat)]
    
  }
}

# test.cols <-  c(id.cols, use.cols, max.cols, maxht.cols, 
#                 "Mixed_Use", "MaxDU_Mixed", "MaxFAR_Mixed", "MaxHt_Mixed",
#                 str_subset(colnames(flu), "_imp$"))
# test <- flu[Mixed_Use == "Y", ..test.cols]

# if max height is missing
# f <- flu[Res_Use == "Y" & is.na(MaxDU_Res) & is.na(MaxHt_Res)]

