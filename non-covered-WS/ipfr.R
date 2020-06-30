# Like ipf.R but using the ipfr package

library(ipfr)

ipf <- function(Margins_, seedAry, maxiter=100, closure=0.01) {
  #Check to see if the sum of each margin is equal
  MarginSums. <- unlist(lapply(Margins_, sum))
  if(any(MarginSums. != MarginSums.[1])) warning("sum of each margin
                                                 not equal")
  
  #Replace margin values of zero with 0.001
  Margins_ <- lapply(Margins_, function(x) {
    if(any(x == 0)) warning("zeros in marginsMtx replaced with
                            0.001") 
    x[x == 0] <- 0.001
    x
  })
  
  #Check to see if number of dimensions in seed array equals the number of
  #margins specified in the marginsMtx
  numMargins <- length(dim(seedAry))
  if(length(Margins_) != numMargins) {
    stop("number of margins in marginsMtx not equal to number of
         margins in seedAry")
  }
  
  ipu_matrix(seedAry, Margins_[[1]], Margins_[[2]], max_iterations = maxiter,
                          relative_gap = closure)
  
}
