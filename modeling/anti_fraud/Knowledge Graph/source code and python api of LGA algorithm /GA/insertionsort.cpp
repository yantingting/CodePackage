/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * insertionsort.cpp
 *
 * Code generation for function 'insertionsort'
 *
 */

/* Include files */
#include "rt_nonfinite.h"
#include "GACD.h"
#include "insertionsort.h"

/* Function Definitions */
void insertionsort(emxArray_int32_T *x, int xstart, int xend, const cell_wrap_1
                   cmp_tunableEnvironment[2])
{
  int k;
  int xc;
  int idx;
  boolean_T exitg1;
  boolean_T varargout_1;
  for (k = xstart; k < xend; k++) {
    xc = x->data[k] - 1;
    idx = k;
    exitg1 = false;
    while ((!exitg1) && (idx >= xstart)) {
      varargout_1 = ((cmp_tunableEnvironment[0].f1->data[xc] <
                      cmp_tunableEnvironment[0].f1->data[x->data[idx - 1] - 1]) ||
                     ((cmp_tunableEnvironment[0].f1->data[xc] ==
                       cmp_tunableEnvironment[0].f1->data[x->data[idx - 1] - 1])
                      && (cmp_tunableEnvironment[1].f1->data[xc] <
                          cmp_tunableEnvironment[1].f1->data[x->data[idx - 1] -
                          1])));
      if (varargout_1) {
        x->data[idx] = x->data[idx - 1];
        idx--;
      } else {
        exitg1 = true;
      }
    }

    x->data[idx] = xc + 1;
  }
}

/* End of code generation (insertionsort.cpp) */
