context("first_true")

test_that("test first_true/last_true", {
    skip_if_not_installed("rqfin")

    x <- c(T,T,T,F,F,F)
    expect_equal(1, first_true(x))
    expect_equal(3, last_true(x))

    x <- c(F,T,T,F,T,F,F)
    expect_equal(2, first_true(x))
    expect_equal(5, last_true(x))

    x <- c(F,F,F,T,T,F,T)
    expect_equal(4, first_true(x))
    expect_equal(7, last_true(x))
})