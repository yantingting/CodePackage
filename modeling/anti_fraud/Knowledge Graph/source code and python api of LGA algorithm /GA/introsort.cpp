/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * introsort.cpp
 *
 * Code generation for function 'introsort'
 *
 */

/* Include files */
#include "rt_nonfinite.h"
#include "GACD.h"
#include "introsort.h"
#include "insertionsort.h"
#include "heapsort.h"

/* Type Definitions */
typedef struct {
  int xstart;
  int xend;
  int depth;
} struct_T;

/* Function Definitions */
void introsort(emxArray_int32_T *x, int xend, const cell_wrap_1
               cmp_tunableEnvironment[2])
{
  int pmax;
  int pmin;
  boolean_T exitg1;
  int MAXDEPTH;
  int p;
  struct_T frame;
  int pow2p;
  struct_T st_d_data[120];
  boolean_T varargout_1;
  int pivot;
  int exitg2;
  int exitg3;
  if (!(1 >= xend)) {
    if (xend <= 32) {
      insertionsort(x, 1, xend, cmp_tunableEnvironment);
    } else {
      pmax = 31;
      pmin = 0;
      exitg1 = false;
      while ((!exitg1) && (pmax - pmin > 1)) {
        p = (pmin + pmax) >> 1;
        pow2p = 1 << p;
        if (pow2p == xend) {
          pmax = p;
          exitg1 = true;
        } else if (pow2p > xend) {
          pmax = p;
        } else {
          pmin = p;
        }
      }

      MAXDEPTH = (pmax - 1) << 1;
      frame.xstart = 1;
      frame.xend = xend;
      frame.depth = 0;
      pmax = MAXDEPTH << 1;
      for (pmin = 0; pmin < pmax; pmin++) {
        st_d_data[pmin] = frame;
      }

      st_d_data[0] = frame;
      p = 1;
      while (p > 0) {
        frame = st_d_data[p - 1];
        p--;
        if ((frame.xend - frame.xstart) + 1 <= 32) {
          insertionsort(x, frame.xstart, frame.xend, cmp_tunableEnvironment);
        } else if (frame.depth == MAXDEPTH) {
          b_heapsort(x, frame.xstart, frame.xend, cmp_tunableEnvironment);
        } else {
          pmax = (frame.xstart + (frame.xend - frame.xstart) / 2) - 1;
          varargout_1 = ((cmp_tunableEnvironment[0].f1->data[x->data[pmax] - 1] <
                          cmp_tunableEnvironment[0].f1->data[x->
                          data[frame.xstart - 1] - 1]) ||
                         ((cmp_tunableEnvironment[0].f1->data[x->data[pmax] - 1]
                           == cmp_tunableEnvironment[0].f1->data[x->
                           data[frame.xstart - 1] - 1]) &&
                          (cmp_tunableEnvironment[1].f1->data[x->data[pmax] - 1]
                           < cmp_tunableEnvironment[1].f1->data[x->
                           data[frame.xstart - 1] - 1])));
          if (varargout_1) {
            pow2p = x->data[frame.xstart - 1];
            x->data[frame.xstart - 1] = x->data[pmax];
            x->data[pmax] = pow2p;
          }

          varargout_1 = ((cmp_tunableEnvironment[0].f1->data[x->data[frame.xend
                          - 1] - 1] < cmp_tunableEnvironment[0].f1->data[x->
                          data[frame.xstart - 1] - 1]) ||
                         ((cmp_tunableEnvironment[0].f1->data[x->data[frame.xend
                           - 1] - 1] == cmp_tunableEnvironment[0].f1->data
                           [x->data[frame.xstart - 1] - 1]) &&
                          (cmp_tunableEnvironment[1].f1->data[x->data[frame.xend
                           - 1] - 1] < cmp_tunableEnvironment[1].f1->data
                           [x->data[frame.xstart - 1] - 1])));
          if (varargout_1) {
            pow2p = x->data[frame.xstart - 1];
            x->data[frame.xstart - 1] = x->data[frame.xend - 1];
            x->data[frame.xend - 1] = pow2p;
          }

          varargout_1 = ((cmp_tunableEnvironment[0].f1->data[x->data[frame.xend
                          - 1] - 1] < cmp_tunableEnvironment[0].f1->data[x->
                          data[pmax] - 1]) || ((cmp_tunableEnvironment[0]
            .f1->data[x->data[frame.xend - 1] - 1] == cmp_tunableEnvironment[0].
            f1->data[x->data[pmax] - 1]) && (cmp_tunableEnvironment[1].f1->
            data[x->data[frame.xend - 1] - 1] < cmp_tunableEnvironment[1]
            .f1->data[x->data[pmax] - 1])));
          if (varargout_1) {
            pow2p = x->data[pmax];
            x->data[pmax] = x->data[frame.xend - 1];
            x->data[frame.xend - 1] = pow2p;
          }

          pivot = x->data[pmax] - 1;
          x->data[pmax] = x->data[frame.xend - 2];
          x->data[frame.xend - 2] = pivot + 1;
          pmax = frame.xstart - 1;
          pmin = frame.xend - 2;
          do {
            exitg2 = 0;
            pmax++;
            do {
              exitg3 = 0;
              varargout_1 = ((cmp_tunableEnvironment[0].f1->data[x->data[pmax] -
                              1] < cmp_tunableEnvironment[0].f1->data[pivot]) ||
                             ((cmp_tunableEnvironment[0].f1->data[x->data[pmax]
                               - 1] == cmp_tunableEnvironment[0].f1->data[pivot])
                              && (cmp_tunableEnvironment[1].f1->data[x->
                                  data[pmax] - 1] < cmp_tunableEnvironment[1].
                                  f1->data[pivot])));
              if (varargout_1) {
                pmax++;
              } else {
                exitg3 = 1;
              }
            } while (exitg3 == 0);

            pmin--;
            do {
              exitg3 = 0;
              varargout_1 = ((cmp_tunableEnvironment[0].f1->data[pivot] <
                              cmp_tunableEnvironment[0].f1->data[x->data[pmin] -
                              1]) || ((cmp_tunableEnvironment[0].f1->data[pivot]
                == cmp_tunableEnvironment[0].f1->data[x->data[pmin] - 1]) &&
                (cmp_tunableEnvironment[1].f1->data[pivot] <
                 cmp_tunableEnvironment[1].f1->data[x->data[pmin] - 1])));
              if (varargout_1) {
                pmin--;
              } else {
                exitg3 = 1;
              }
            } while (exitg3 == 0);

            if (pmax + 1 >= pmin + 1) {
              exitg2 = 1;
            } else {
              pow2p = x->data[pmax];
              x->data[pmax] = x->data[pmin];
              x->data[pmin] = pow2p;
            }
          } while (exitg2 == 0);

          x->data[frame.xend - 2] = x->data[pmax];
          x->data[pmax] = pivot + 1;
          if (pmax + 2 < frame.xend) {
            st_d_data[p].xstart = pmax + 2;
            st_d_data[p].xend = frame.xend;
            st_d_data[p].depth = frame.depth + 1;
            p++;
          }

          if (frame.xstart < pmax + 1) {
            st_d_data[p].xstart = frame.xstart;
            st_d_data[p].xend = pmax + 1;
            st_d_data[p].depth = frame.depth + 1;
            p++;
          }
        }
      }
    }
  }
}

/* End of code generation (introsort.cpp) */
