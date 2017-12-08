context("ffill")

test_that("test ffill", {
    skip_if_not_installed("rqfin")

    n <- NA

    x <- c(n,1,2,n,n,3,n,4,n,n)
    s <- c(n,1,2,2,2,3,3,4,4,4)
    expect_equal(s, ffill(x))

    x <- c(n,1,2,n,n,n,3,n,n,n,n,4,n,n)
    s <- c(n,1,2,2,2,n,3,3,3,n,n,4,4,4)
    expect_equal(s, ffill(x, 2))    

})