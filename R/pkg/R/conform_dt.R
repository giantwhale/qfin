#' Make All groups the Same Shape
#' 
#' @param x a data.frame/data.table object
#' @param group.by column names or column positions
#' @param index.by column names or column positions
#' @param reorder.cols 
#' 
#' @return a data.table object
#' @importFrom data.table as.data.table 
#' @export
conform_dt <- function(x, group.by, index.by, reorder.cols=TRUE) {
    x <- as.data.table(x)
    if (is.numeric(group.by)) {
        group.by <- names(x)[group.by]
    }
    if (is.numeric(index.by)) {
        index.by <- names(x)[index.by]
    }
    all.names <- c(group.by, index.by)

    dups <- duplicated(all.names)
    if (any(dups)) {
        stop(sprintf('Duplicated column name: %s', all.names[dups][1]))
    }
    
    setkeyv(x, all.names)
    all.index <- unique(x[, index.by, with=FALSE])[, '__KEY__' := 1L]
    all.group <- unique(x[, group.by, with=FALSE])[, '__KEY__' := 1L]
    full.keys <- merge(all.group, all.index, by='__KEY__', allow.cartesian=TRUE)
    full.keys <- full.keys[, all.names, with=FALSE]

    setkeyv(full.keys, all.names)
    setkeyv(x, all.names)

    full.x <- x[J(full.keys), ]
    if (reorder.cols) {
        other.names <- names(full.x)[!names(full.x) %chin% all.names]
        full.x <- full.x[, c(all.names, other.names), with=FALSE]
    }
    full.x
}
