#' Count Number of NAs in Each Element of x
#' @export 
count_na <- function(x) {
    if (is.vector(x)) {
        n <- as.character(sum(is.na(x)))
        n[n == '0'] <- '-'
        w <- max(nchar(n))
        fmt <- sprintf('NA Count: %%ds\n', w)
        cat(sprintf(fmt, n))
    } else if (is.list(x)) {
        df <- data.frame(Name  = names(x)
                       , Count = as.character(sapply(x, function(x) sum(is.na(x))))
                       , stringsAsFactors = FALSE)
        df$Count[df$Count == '0'] <- '-'
        df[] <- sapply(df, function(x) {
                w <- max(nchar(x))
                fmt <- sprintf('%%%ds', w)
                sprintf(fmt, x)
            })
        out <- paste(paste0(df$Name, ': ', df$Count), collapse='\n')
        cat(out, '\n')
    } else{
        stop(sprintf('Object type is not supported: %s', class(x)))
    }
}
