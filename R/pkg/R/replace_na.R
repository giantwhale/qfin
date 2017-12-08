#' @export 
replace_na <- function(x, val=0) {
    replace(x, is.na(x), val)
}
