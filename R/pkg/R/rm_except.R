#' Remove all variables except ones specified
#' 
#' @param ... the objects to be removed, as names (unquoted) or character strings (quoted)
#' @param list a character vector naming objects to be kept
#' @param all will be paased to the \code{ls()} function
#' 
#' @importFrom data.table %chin%
#' @export 
rm_except <- function(..., list=character(), all=FALSE) {
    dots <- match.call(expand.dots = FALSE)$...
    if (length(dots) && !all(vapply(dots, function(x) is.symbol(x) ||
        is.character(x), NA, USE.NAMES=FALSE)))
        stop("... must contain names or character strings")
    names <- vapply(dots, as.character, "")
    if (length(names) == 0L)
        names <- character()
    list <- c(list, names)

    obj <- ls(all=all, envir=parent.frame())
    obj <- obj[!obj %chin% list]

    rm(list=obj, envir=parent.frame())
}