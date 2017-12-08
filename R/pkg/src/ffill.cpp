#include <iostream>
#include <Rcpp.h>
using namespace Rcpp;

//' Forward Fill NAs 
//'
//' @param x numeric vector
//' @param w integer, windows size, results are right aligned if w < 0, left aligned if w > 0
//'
//' @export
// [[Rcpp::export]]
NumericVector ffill(NumericVector x, int w=0) {
    size_t sz  = x.size();
    NumericVector res(sz, NA_REAL);

    if (w <= 0) {
        w = sz;
    }

    size_t cnt = 0;
    double v   = NA_REAL;
    for (size_t i=0; i<sz; ++i) {
        ++cnt;

        if (NumericVector::is_na(x[i])) {
            if (cnt <= w) {
                res[i] = v;
            }
        } else {
            v   = x[i];
            cnt = 0;
            res[i] = v;
        }

    }

    return res;

}
