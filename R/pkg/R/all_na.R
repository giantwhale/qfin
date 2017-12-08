#' Element-wise Test if all Values are NAs
#' 
#' @export
all_na <- function(...) {
    vars <- list(...)
    Reduce('&', lapply(vars, is.na))
}


#' Element-wise Test if any Values are NAs
#' 
#' @export
any_na <- function(...) {
    vars <- list(...)
    Reduce('|', lapply(vars, is.na))
}
