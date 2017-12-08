#' An R Library for Quant Financial Analysis
#' 
#' @name rqfin
#' @docType package
#' @import Rcpp
#' @useDynLib rqfin
NULL

.onUnload <- function(libpath) {
    cat("Unloading Dynlib rqfin ...\n")
    library.dynam.unload('rqfin', libpath)
}