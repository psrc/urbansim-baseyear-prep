ipf <- function(Margins_, seedAry, maxiter=100, closure=0.001) {
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

    #Set initial values
    resultAry <- seedAry
    iter <- 0
    marginChecks <- rep(1, numMargins)
    margins <- seq(1, numMargins)
    #Iteratively proportion margins until closure or iteration criteria are met
    while((any(marginChecks > closure)) & (iter < maxiter)) {
        for(margin in margins) {
            marginTotal <- apply(resultAry, margin, sum)
            marginCoeff <- Margins_[[margin]]/marginTotal
            marginCoeff[is.infinite(marginCoeff)] <- 0
            resultAry <- sweep(resultAry, margin, marginCoeff, "*")
            marginChecks[margin] <- sum(abs(1 - marginCoeff))
        }    
        iter <- iter + 1
    }
	#print(max(marginChecks))
    #If IPF stopped due to number of iterations then output info
    if(iter == maxiter) cat("IPF stopped due to number of iterations\n")

    #Return balanced array
    resultAry
    }
