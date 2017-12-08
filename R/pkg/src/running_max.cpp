#include <iostream>
#include <deque>
#include <utility>
#include <algorithm>

#include <Rcpp.h>

using std::deque;
using std::pair;
using std::make_pair;
using namespace Rcpp;

//' Running Min
//'
//' This O(N) time solution is borrowed from Stackoverflow, I think it is 
//' easier to implement than the two array solution
//'
//' at every step:
//' 
//'   if (!Deque.Empty) and (Deque.Head.Index <= CurrentIndex - T) then 
//'      Deque.ExtractHead;
//'   //Head is too old, it is leaving the window
//' 
//'   while (!Deque.Empty) and (Deque.Tail.Value > CurrentValue) do
//'      Deque.ExtractTail;
//'   //remove elements that have no chance to become minimum in the window
//' 
//'   Deque.AddTail(CurrentValue, CurrentIndex); 
//'   CurrentMin = Deque.Head.Value
//'   //Head value is minimum in the current window
//'  
//'
//' @param x numeric vector
//' @param w integer, windows size, results are right aligned if w < 0, left aligned if w > 0
//'
//' @export
// [[Rcpp::export]]
NumericVector running_min(NumericVector x, int w, size_t min_size=1) {
    size_t sz  = x.size();
    NumericVector res(sz, NA_REAL);
    pair<double, int> el;
    deque<pair<double, int> > q;

    if (w == 0) throw std::range_error("window size w must be non-zero");
    if (min_size <= 0) throw std::range_error("min_size must be positive");

    if (w > 0) {
        // right-aligned
        NumericVector revX = clone<NumericVector>(x);
        std::reverse(revX.begin(), revX.end());
        res = running_min(revX, -w, min_size);
        std::reverse(res.begin(), res.end());
        return res;
    }

    w = -w;
    
    if (w > sz) throw std::range_error("Invalid window size");

    size_t cnt = 0;

    for (int i=0; i<sz; ++i) {

        double v = x[i];
        bool is_na = NumericVector::is_na(v);

        if (!is_na) {
            ++cnt;
        }
        if (i >= w && !NumericVector::is_na(x[i-w])) {
            --cnt;
        }

        if (!q.empty()) {
            //Head is too old, it is leaving the window
            el = q.front();
            if (el.second <= i - w) {
                q.pop_front();
            }
        }

        //remove elements that have no chance to become minimum in the window
        if (!NumericVector::is_na(v)) {
            while (!q.empty()) {
                el = q.back();
                if (el.first > v) {
                    q.pop_back();
                } else {
                    break;
                }

            }                    
        }

        if (!NumericVector::is_na(v)) {
            q.push_back(make_pair<double, int>(v, i));
        }

        //Head value is minimum in the current window
        if (cnt >= min_size && !q.empty()) {
            el = q.front();
            res[i] = el.first;                
        }
    }

    return res;
}



// [[Rcpp::export]]
NumericVector running_max(NumericVector x, int w, size_t min_size=1) {
    NumericVector res = running_min(-x, w, min_size);
    return -res;
}
