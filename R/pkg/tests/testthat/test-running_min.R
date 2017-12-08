context("running_sd")

test_that("running_sd, left aligned", {
    skip_if_not_installed("rqfin")

    n <- NA

    set.seed(527)

    x <- c(1,2,3,4,3,2,1,3,4,5,6,7,1)
    s <- c(1,2,3,4,4,4,3,3,4,5,6,7,7)
    expect_equal(s, running_max(x, -3))


    x <- c(1,2,3,n,5,n,n,n,4,5,6,7,1,n)
    s <- c(n,2,3,3,5,n,n,n,n,5,6,7,7,7)
    expect_equal(s, running_max(x, -3, 2))


    x <- c(1,2,3,4,5,6,7, 7 + cumsum(rnorm(10000)))
    s <- c(1,2,3,4,5,6,7, rep(NA, 10000))
    for (i in 8:10007) {
        s[i] <- max(x[(i-6):i])
    }
    expect_equal(s, running_max(x, -7))


    x <- c(1,2,3,4,5,6,7, 7 + cumsum(rnorm(10000)))
    s <- c(1,1,1,1,1,1,1, rep(NA, 10000))
    for (i in 8:10007) {
        s[i] <- min(x[(i-6):i])
    }
    expect_equal(s, running_min(x, -7))

})


test_that("running_sd, right aligned", {
    skip_if_not_installed("rqfin")

    n <- NA

    set.seed(527)

    x <- c(1,2,3,4,3,2,1,3,4,5,6,7,1)
    s <- c(3,4,4,4,3,3,4,5,6,7,7,7,1)
    expect_equal(s, running_max(x, 3))

    x <- c(cumsum(rnorm(10000)))
    s <- rep(NA, 10000)
    for (i in 1:10000) {
        end <- min(10000, i + 6)
        s[i] <- max(x[i:end])
    }
    expect_equal(s, running_max(x, 7))

    x[sample(10000, 200)] <- n
    x[600:610] <- n
    s <- rep(NA, 10000)
    for (i in 1:10000) {
        end <- min(10000, i + 6)
        v <- x[i:end]
        if (sum(!is.na(v)) >= 5) {
            s[i] <- max(v, na.rm=TRUE)
        }
    }
    expect_equal(s, running_max(x, 7, 5))

})