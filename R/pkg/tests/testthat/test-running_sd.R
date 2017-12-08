context("running_sd")

test_that("running_sd", {
    skip_if_not_installed("rqfin")

    n <- NA

    set.seed(527)

    # left aligned
    x <- runif(100)
    s <- rep(NA, 100)
    for (i in 5:9) {
        s[i] <- sd(x[1:i])
    }
    for (i in 10:100) {
        s[i] <- sd(x[(i-9):i])
    }
    expect_equal(s, running_sd(x, -10, 5))


    # left aligned with NA
    x <- runif(100)
    x[50:60] <- NA
    x[sample(100, 20)] <- NA
    s <- rep(NA, 100)
    for (i in 5:9) {
        v <- x[1:i]
        if (sum(!is.na(v)) >= 5) {
            s[i] <- sd(v, na.rm=TRUE)
        }
    }
    for (i in 10:100) {
        v <- x[(i-9):i]
        if (sum(!is.na(v)) >= 5) {
            s[i] <- sd(v, na.rm=TRUE)
        }

    }
    expect_equal(s, running_sd(x, -10, 5))


    # right aligned
    x <- runif(100)
    s <- rep(NA, 100)
    for (i in 1:90) {
        s[i] <- sd(x[i:(i+9)])
    }
    for (i in 91:96) {
        s[i] <- sd(x[i:100])
    }
    expect_equal(s, running_sd(x, 10, 5))

    # right aligned with NA
    x <- runif(100)
    x[50:60] <- NA
    x[sample(100, 20)] <- NA
    s <- rep(NA, 100)
    for (i in 1:90) {
        v <- x[i:(i+9)]
        if (sum(!is.na(v)) >= 5) {
            s[i] <- sd(v, na.rm=TRUE)
        }
    }
    for (i in 91:96) {
        v <- x[i:100]
        if (sum(!is.na(v)) >= 5) {
            s[i] <- sd(v, na.rm=TRUE)
        }

    }
    expect_equal(s, running_sd(x, 10, 5))


})




test_that("running_sd0", {
    skip_if_not_installed("rqfin")

    sd <- function(x, na.rm=NA) {
        # na.rm is not used
        if (sum(!is.na(x)) == 0) {
            return (NA)
        }
        x <- x[!is.na(x)]
        sqrt(mean(x * x))
    }

    n <- NA

    set.seed(527)

    # left aligned
    x <- runif(100)
    s <- rep(NA, 100)
    for (i in 5:9) {
        s[i] <- sd(x[1:i])
    }
    for (i in 10:100) {
        s[i] <- sd(x[(i-9):i])
    }
    expect_equal(s, running_sd0(x, -10, 5))


    # left aligned with NA
    x <- runif(100)
    x[50:60] <- NA
    x[sample(100, 20)] <- NA
    s <- rep(NA, 100)
    for (i in 5:9) {
        v <- x[1:i]
        if (sum(!is.na(v)) >= 5) {
            s[i] <- sd(v, na.rm=TRUE)
        }
    }
    for (i in 10:100) {
        v <- x[(i-9):i]
        if (sum(!is.na(v)) >= 5) {
            s[i] <- sd(v, na.rm=TRUE)
        }

    }
    expect_equal(s, running_sd0(x, -10, 5))


    # right aligned
    x <- runif(100)
    s <- rep(NA, 100)
    for (i in 1:90) {
        s[i] <- sd(x[i:(i+9)])
    }
    for (i in 91:96) {
        s[i] <- sd(x[i:100])
    }
    expect_equal(s, running_sd0(x, 10, 5))

    # right aligned with NA
    x <- runif(100)
    x[50:60] <- NA
    x[sample(100, 20)] <- NA
    s <- rep(NA, 100)
    for (i in 1:90) {
        v <- x[i:(i+9)]
        if (sum(!is.na(v)) >= 5) {
            s[i] <- sd(v, na.rm=TRUE)
        }
    }
    for (i in 91:96) {
        v <- x[i:100]
        if (sum(!is.na(v)) >= 5) {
            s[i] <- sd(v, na.rm=TRUE)
        }

    }
    expect_equal(s, running_sd0(x, 10, 5))

})