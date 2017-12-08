#include <iostream>
#include <Rcpp.h>
using namespace Rcpp;

//' Running Exponential Moving Average (EMA)
//'
//' Missing Values are forward filled. 
//'
//' @param x numeric vector
//' @param decay double, the decay factor
//' @param min_size if number of non-NA elements is fewer than min_size, return NA. For EMA,
//'    this parameter is only used to remove the first a few elements.
//'
//' @export
// [[Rcpp::export]]
NumericVector exp_moving_avg(NumericVector x, double decay, int min_size=1) {
    size_t sz  = x.size();
    NumericVector res(sz, NA_REAL);

    if (decay <= 0 || decay >= 1) throw std::range_error("decay must be between 0 and 1");
    if (min_size <= 0) throw std::range_error("min_size must be positive");

    size_t cnt  = 0;  // non_na counts
    double s    = 0;
    double totw = 0;
    for (size_t i=0; i<sz; ++i) {
        if (!NumericVector::is_na(x[i])) {
            ++cnt;
            s    = x[i] + decay * s;
            totw = 1.0  + decay * totw;
        }
        if (cnt >= min_size) {
            res[i] = s / totw;
        }
    }
    return res;
}
