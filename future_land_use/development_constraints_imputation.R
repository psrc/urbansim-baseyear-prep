# Impute development constraints and compare to previous density table
library(stringr)
library(purrr)
library(magrittr)
library(data.table)
library(foreign)


# read new FLU and old FLU files
flu <- fread("W:/gis/projects/compplan_zoning/density_table_4_gis.csv")
oflu <- read.dbf("X:/DSA/FutureLandUse/FLU_2016/FLU_for_DC.dbf", as.is = TRUE) %>% as.data.table
# old.flu <- fread("C:/Users/clam/Desktop/urbansim-baseyear-prep/future_land_use/flu_for_dc_2016.csv")

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

# New flu, impute ---------------------------------------------------------


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


# Compile previous flu ----------------------------------------------------


# clean old flu (2016)
oflu[, Max_Height := as.numeric(Max_Height)]

oflu.max.cols <- str_subset(colnames(oflu), "^Max_.*")
oflu.cols <- c("Jurisdicti", "Key", "Definition", oflu.max.cols)
oflu <- oflu[, ..oflu.cols]

# join flu to old flu (2016)
# _new = flu, _prev = old flu
flu.join <- merge(flu, oflu, by = c("Key"), suffixes = c("_new", "_prev"), all = TRUE)
setnames(flu.join, oflu.max.cols, paste0(oflu.max.cols, "_prev"))


# Compare collected, imputed, previous flu vals --------------------------


# overall tally matches/unmatched
flu.join[is.na(Jurisdicti_new), .N] # unmatched flu
flu.join[is.na(Jurisdicti_prev), .N] # unmatched old flu
flu.join[!is.na(Jurisdicti_prev) & !is.na(Jurisdicti_new), .N] # matched

# flu comparison
comp.cols <- c("Key", 
               str_subset(colnames(flu.join), "_new"),
               use.cols,
               max.cols,
               maxht.cols,
               unname(unlist(flatten(cols.sets[['Mixed']]))),
               str_subset(colnames(flu.join), "_imp"),
               str_subset(colnames(flu.join), "_prev"))
comp.flu <- flu.join[!is.na(Jurisdicti_new), ..comp.cols]
setcolorder(comp.flu, c("Key", 
                        str_subset(colnames(comp.flu), "_new"),
                        str_subset(colnames(comp.flu), "Res"),
                        str_subset(colnames(comp.flu), "Comm"),
                        str_subset(colnames(comp.flu), "Indust"),
                        str_subset(colnames(comp.flu), "Office"),
                        str_subset(colnames(comp.flu), "Mixed"),
                        str_subset(colnames(comp.flu), "_prev")))

# export comp.flu for review
# write.csv(comp.flu, "T:/2020October/christy/baseyear/impute_flu_comparison.csv", row.names = F)

# tally how many are na, how many are na but have prev DU or FAR 
for (i in 1:length(cols.sets)) {
  print(names(cols.sets[i]))
  s <- cols.sets[[i]]

  for (j in s$dens) {
    use.col <-s$use
    ht.col <- s$height
    
    ifelse(str_detect(j, "DU"), prev.dens.col <- "Max_DU_Ac_prev", prev.dens.col <- "Max_FAR_prev")
    
    if (names(cols.sets[i]) == "Mixed") {
      ifelse(str_detect(j, "DU"), density.col <- s$dens$du, density.col <- s$dens$far)
    } else {
      density.col <- s$dens
    }
    
    cat("total na remaining: ", 
        comp.flu[get(eval(use.col)) == "Y" & is.na(get(eval(paste0(density.col, "_imp")))) & is.na(get(eval(density.col))) & is.na(get(eval(ht.col))), .N],
        "\n")
    
    cat("na but has density from 2016: ", 
        comp.flu[get(eval(use.col)) == "Y" & is.na(get(eval(paste0(density.col, "_imp")))) & is.na(get(eval(density.col))) & is.na(get(eval(ht.col))) & (get(eval(prev.dens.col)) > 0) , .N],
        "\n\n")
  }          
}


# Update remaining na in _imp columns if prev values available -------------


# if max height is missing 
# loop thru cols.sets, update col ending '_imp' with prev du/far if available
# optional to copy original Max du/far to '_imp' cols
for (i in 1:length(cols.sets)) {
  print(names(cols.sets[i]))
  s <- cols.sets[[i]]
  
  for (j in s$dens) {
    use.col <-s$use
    ht.col <- s$height
    
    ifelse(str_detect(j, "DU"), prev.dens.col <- "Max_DU_Ac_prev", prev.dens.col <- "Max_FAR_prev")
    
    if (names(cols.sets[i]) == "Mixed") {
      ifelse(str_detect(j, "DU"), density.col <- s$dens$du, density.col <- s$dens$far)
    } else {
      density.col <- s$dens
    }
    
    imp.density.col <- paste0(density.col, "_imp")

    # update col ending '_imp' with prev du/far
    flu.join[get(eval(use.col)) == "Y" & 
               is.na(get(eval(imp.density.col))) & 
               is.na(get(eval(density.col))) & 
               is.na(get(eval(ht.col))) & 
               (get(eval(prev.dens.col)) > 0) , (imp.density.col) := get(eval(prev.dens.col))]
    
    # # update col ending '_imp' with original du/far
    # flu.join[get(eval(use.col)) == "Y" &
    #            is.na(get(eval(imp.density.col))) &
    #            !is.na(get(eval(density.col))), (imp.density.col) := get(eval(density.col))]

  } 
}


# QC
# f.cols <- c(str_subset(colnames(comp.flu), "Juris"), str_subset(colnames(comp.flu), "Res"), str_subset(colnames(comp.flu), "_prev"))
# f <- comp.flu[Res_Use == "Y" & is.na(MaxDU_Res) & is.na(MaxDU_Res_imp) & is.na(MaxHt_Res) & (Max_DU_Ac_prev > 0)]
# f <- comp.flu[Res_Use == "Y" & is.na(MaxDU_Res) & is.na(MaxDU_Res_imp) & is.na(MaxHt_Res), ..f.cols]
# f <- flu.join[Res_Use == "Y" & is.na(MaxDU_Res) & is.na(MaxHt_Res) & (Max_DU_Ac_prev > 0), ..f.cols]
# f <- flu.join[Res_Use == "Y" & !is.na(MaxDU_Res) & is.na(MaxDU_Res_imp), ..f.cols]
# f <- flu.join[Jurisdicti_new == "Bellevue" & Res_Use == "Y", ..f.cols]

