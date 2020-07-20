/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * Mutation.cpp
 *
 * Code generation for function 'Mutation'
 *
 */

/* Include files */
#include <cmath>
#include "rt_nonfinite.h"
#include "GACD.h"
#include "Mutation.h"
#include "GACD_emxutil.h"
#include "sum.h"
#include "rand.h"
#include "diff.h"
#include "sort1.h"
#include "compute_Q2.h"
#include "find_communities.h"
#include "convert_labels.h"

/* Function Definitions */
void Mutation(const emxArray_real_T *W, const emxArray_real_T *degrees, double m,
              const emxArray_real_T *jin, emxArray_real_T *pop2, emxArray_real_T
              *pop2_Q, double pop2_size)
{
  int num;
  emxArray_real_T *o_labels;
  emxArray_real_T *vs;
  emxArray_real_T *V;
  emxArray_real_T *new_labels;
  emxArray_real_T *ff;
  emxArray_real_T *nn2;
  emxArray_real_T *nn;
  emxArray_real_T *colors;
  emxArray_real_T *b_index;
  emxArray_real_T *indexs;
  emxArray_real_T *di;
  emxArray_int32_T *gg;
  emxArray_int32_T *r4;
  emxArray_real_T *difference;
  emxArray_boolean_T *x;
  emxArray_int32_T *ii;
  emxArray_real_T *b_ff;
  emxArray_real_T *b_degrees;
  int idx;
  int i11;
  int nx;
  int i;
  int exitg1;
  int b_new_labels;
  int i12;
  int i13;
  int rr;
  double ma;
  double val;
  boolean_T exitg2;
  num = 0;
  emxInit_real_T(&o_labels, 2);
  emxInit_real_T(&vs, 2);
  emxInit_real_T(&V, 2);
  emxInit_real_T(&new_labels, 2);
  emxInit_real_T(&ff, 2);
  emxInit_real_T(&nn2, 2);
  emxInit_real_T(&nn, 2);
  emxInit_real_T(&colors, 2);
  emxInit_real_T(&b_index, 2);
  emxInit_real_T(&indexs, 2);
  emxInit_real_T(&di, 2);
  emxInit_int32_T1(&gg, 2);
  emxInit_int32_T(&r4, 1);
  emxInit_real_T(&difference, 2);
  emxInit_boolean_T(&x, 2);
  emxInit_int32_T1(&ii, 2);
  emxInit_real_T(&b_ff, 2);
  emxInit_real_T(&b_degrees, 2);
  while (num <= (int)pop2_size - 1) {
    idx = pop2->size[1];
    i11 = o_labels->size[0] * o_labels->size[1];
    o_labels->size[0] = 1;
    o_labels->size[1] = idx;
    emxEnsureCapacity_real_T(o_labels, i11);
    for (i11 = 0; i11 < idx; i11++) {
      o_labels->data[o_labels->size[0] * i11] = pop2->data[num + pop2->size[0] *
        i11];
    }

    nx = pop2->size[1];
    i11 = vs->size[0] * vs->size[1];
    vs->size[0] = 1;
    vs->size[1] = nx;
    emxEnsureCapacity_real_T(vs, i11);
    for (i11 = 0; i11 < nx; i11++) {
      vs->data[i11] = 0.0;
    }

    i = 0;
    do {
      exitg1 = 0;
      i11 = pop2->size[1] - 1;
      if (i <= i11) {
        if (1.0 + (double)i != o_labels->data[i]) {
          vs->data[(int)o_labels->data[i] - 1]++;
        }

        i++;
      } else {
        exitg1 = 1;
      }
    } while (exitg1 == 0);

    convert_labels(o_labels, new_labels);
    nx = new_labels->size[1];
    i11 = V->size[0] * V->size[1];
    V->size[0] = 1;
    V->size[1] = new_labels->size[1];
    emxEnsureCapacity_real_T(V, i11);
    idx = new_labels->size[1];
    for (i11 = 0; i11 < idx; i11++) {
      V->data[i11] = 0.0;
    }

    find_communities(new_labels, indexs, di);
    b_new_labels = new_labels->size[1];
    i11 = new_labels->size[0] * new_labels->size[1];
    new_labels->size[0] = 1;
    new_labels->size[1] = b_new_labels;
    emxEnsureCapacity_real_T(new_labels, i11);
    for (i11 = 0; i11 < b_new_labels; i11++) {
      new_labels->data[new_labels->size[0] * i11] = 0.0;
    }

    for (i = 0; i <= di->size[1] - 2; i++) {
      if (di->data[i] > di->data[i + 1] - 1.0) {
        i11 = 0;
        i12 = 0;
      } else {
        i11 = (int)di->data[i] - 1;
        i12 = (int)(di->data[i + 1] - 1.0);
      }

      i13 = ii->size[0] * ii->size[1];
      ii->size[0] = 1;
      ii->size[1] = i12 - i11;
      emxEnsureCapacity_int32_T(ii, i13);
      idx = i12 - i11;
      for (i12 = 0; i12 < idx; i12++) {
        ii->data[ii->size[0] * i12] = (int)indexs->data[i11 + i12];
      }

      idx = ii->size[0] * ii->size[1] - 1;
      for (i11 = 0; i11 <= idx; i11++) {
        new_labels->data[ii->data[i11] - 1] = i + 1;
      }
    }

    c_rand((double)nx, ff);
    sort(ff, ii);
    i11 = gg->size[0] * gg->size[1];
    gg->size[0] = 1;
    gg->size[1] = ii->size[1];
    emxEnsureCapacity_int32_T(gg, i11);
    idx = ii->size[0] * ii->size[1];
    for (i11 = 0; i11 < idx; i11++) {
      gg->data[i11] = ii->data[i11];
    }

    i11 = new_labels->size[1];
    for (rr = 0; rr < i11; rr++) {
      i = gg->data[rr];
      if (vs->data[gg->data[rr] - 1] == 0.0) {
        /* %%%% */
        /*          tp=0.5; */
        /*          if rand()>tp */
        /*              continue; */
        /*          end */
        /* %%%% */
        if (jin->data[gg->data[rr] - 1] > jin->data[gg->data[rr]] - 1.0) {
          i12 = 0;
          i13 = 0;
        } else {
          i12 = (int)jin->data[gg->data[rr] - 1] - 1;
          i13 = (int)(jin->data[gg->data[rr]] - 1.0);
        }

        b_new_labels = nn2->size[0] * nn2->size[1];
        nn2->size[0] = 2;
        nn2->size[1] = i13 - i12;
        emxEnsureCapacity_real_T(nn2, b_new_labels);
        idx = i13 - i12;
        for (i13 = 0; i13 < idx; i13++) {
          for (b_new_labels = 0; b_new_labels < 2; b_new_labels++) {
            nn2->data[b_new_labels + nn2->size[0] * i13] = W->data[(i12 + i13) +
              W->size[0] * (b_new_labels << 1)];
          }
        }

        idx = nn2->size[1];
        i12 = nn->size[0] * nn->size[1];
        nn->size[0] = 1;
        nn->size[1] = idx;
        emxEnsureCapacity_real_T(nn, i12);
        for (i12 = 0; i12 < idx; i12++) {
          nn->data[nn->size[0] * i12] = nn2->data[nn2->size[0] * i12];
        }

        i12 = ff->size[0] * ff->size[1];
        ff->size[0] = 1;
        ff->size[1] = nn->size[1];
        emxEnsureCapacity_real_T(ff, i12);
        idx = nn->size[0] * nn->size[1];
        for (i12 = 0; i12 < idx; i12++) {
          ff->data[i12] = new_labels->data[(int)nn->data[i12] - 1];
        }

        b_sort(ff);
        i12 = b_ff->size[0] * b_ff->size[1];
        b_ff->size[0] = 1;
        b_ff->size[1] = ff->size[1] + 1;
        emxEnsureCapacity_real_T(b_ff, i12);
        idx = ff->size[1];
        for (i12 = 0; i12 < idx; i12++) {
          b_ff->data[b_ff->size[0] * i12] = ff->data[ff->size[0] * i12];
        }

        b_ff->data[b_ff->size[0] * ff->size[1]] = rtNaN;
        diff(b_ff, difference);
        idx = difference->size[1] - 1;
        b_new_labels = 0;
        for (nx = 0; nx <= idx; nx++) {
          if (difference->data[nx] != 0.0) {
            b_new_labels++;
          }
        }

        i12 = colors->size[0] * colors->size[1];
        colors->size[0] = 1;
        colors->size[1] = b_new_labels;
        emxEnsureCapacity_real_T(colors, i12);
        b_new_labels = 0;
        for (nx = 0; nx <= idx; nx++) {
          if (difference->data[nx] != 0.0) {
            colors->data[b_new_labels] = ff->data[nx];
            b_new_labels++;
          }
        }

        ma = -1.7976931348623157E+308;
        i12 = ff->size[0] * ff->size[1];
        ff->size[0] = 1;
        ff->size[1] = 1;
        emxEnsureCapacity_real_T(ff, i12);
        ff->data[0] = -1.0;
        idx = nn2->size[1];
        i12 = ii->size[0] * ii->size[1];
        ii->size[0] = 1;
        ii->size[1] = idx;
        emxEnsureCapacity_int32_T(ii, i12);
        for (i12 = 0; i12 < idx; i12++) {
          ii->data[ii->size[0] * i12] = (int)nn2->data[nn2->size[0] * i12];
        }

        idx = nn2->size[1] - 1;
        for (i12 = 0; i12 <= idx; i12++) {
          V->data[ii->data[ii->size[0] * i12] - 1] = nn2->data[1 + nn2->size[0] *
            i12];
        }

        for (nx = 0; nx < colors->size[1]; nx++) {
          if (di->data[(int)colors->data[nx] - 1] > di->data[(int)(colors->
               data[nx] + 1.0) - 1] - 1.0) {
            i12 = 0;
            i13 = 0;
          } else {
            i12 = (int)di->data[(int)colors->data[nx] - 1] - 1;
            i13 = (int)(di->data[(int)(colors->data[nx] + 1.0) - 1] - 1.0);
          }

          b_new_labels = difference->size[0] * difference->size[1];
          difference->size[0] = 1;
          difference->size[1] = i13 - i12;
          emxEnsureCapacity_real_T(difference, b_new_labels);
          idx = i13 - i12;
          for (i13 = 0; i13 < idx; i13++) {
            difference->data[difference->size[0] * i13] = indexs->data[i12 + i13];
          }

          if (colors->data[nx] != new_labels->data[i - 1]) {
            i12 = b_index->size[0] * b_index->size[1];
            b_index->size[0] = 1;
            b_index->size[1] = 1 + difference->size[1];
            emxEnsureCapacity_real_T(b_index, i12);
            b_index->data[0] = i;
            idx = difference->size[1];
            for (i12 = 0; i12 < idx; i12++) {
              b_index->data[b_index->size[0] * (i12 + 1)] = difference->
                data[difference->size[0] * i12];
            }
          } else {
            i12 = b_index->size[0] * b_index->size[1];
            b_index->size[0] = 1;
            b_index->size[1] = difference->size[1];
            emxEnsureCapacity_real_T(b_index, i12);
            idx = difference->size[0] * difference->size[1];
            for (i12 = 0; i12 < idx; i12++) {
              b_index->data[i12] = difference->data[i12];
            }
          }

          i12 = b_ff->size[0] * b_ff->size[1];
          b_ff->size[0] = 1;
          b_ff->size[1] = b_index->size[1];
          emxEnsureCapacity_real_T(b_ff, i12);
          idx = b_index->size[0] * b_index->size[1];
          for (i12 = 0; i12 < idx; i12++) {
            b_ff->data[i12] = V->data[(int)b_index->data[i12] - 1];
          }

          i12 = b_degrees->size[0] * b_degrees->size[1];
          b_degrees->size[0] = 1;
          b_degrees->size[1] = b_index->size[1];
          emxEnsureCapacity_real_T(b_degrees, i12);
          val = degrees->data[gg->data[rr] - 1];
          idx = b_index->size[0] * b_index->size[1];
          for (i12 = 0; i12 < idx; i12++) {
            b_degrees->data[i12] = val * degrees->data[(int)b_index->data[i12] -
              1];
          }

          val = b_sum(b_ff) - b_sum(b_degrees) / (2.0 * m);
          if (val == ma) {
            b_new_labels = ff->size[1];
            i12 = ff->size[0] * ff->size[1];
            ff->size[1] = b_new_labels + 1;
            emxEnsureCapacity_real_T(ff, i12);
            ff->data[b_new_labels] = colors->data[nx];
          }

          if (val > ma) {
            ma = val;
            i12 = ff->size[0] * ff->size[1];
            ff->size[0] = 1;
            ff->size[1] = 1;
            emxEnsureCapacity_real_T(ff, i12);
            ff->data[0] = colors->data[nx];
          }
        }

        idx = nn2->size[1];
        i12 = ii->size[0] * ii->size[1];
        ii->size[0] = 1;
        ii->size[1] = idx;
        emxEnsureCapacity_int32_T(ii, i12);
        for (i12 = 0; i12 < idx; i12++) {
          ii->data[ii->size[0] * i12] = (int)nn2->data[nn2->size[0] * i12];
        }

        idx = ii->size[0] * ii->size[1] - 1;
        for (i12 = 0; i12 <= idx; i12++) {
          V->data[ii->data[i12] - 1] = 0.0;
        }

        if (ff->size[1] == 1) {
          new_labels->data[gg->data[rr] - 1] = ff->data[0];
        } else {
          val = (double)ff->size[1] * b_rand();
          new_labels->data[gg->data[rr] - 1] = ff->data[(int)std::ceil(val) - 1];
        }

        i12 = x->size[0] * x->size[1];
        x->size[0] = 1;
        x->size[1] = nn->size[1];
        emxEnsureCapacity_boolean_T(x, i12);
        val = new_labels->data[gg->data[rr] - 1];
        idx = nn->size[0] * nn->size[1];
        for (i12 = 0; i12 < idx; i12++) {
          x->data[i12] = (new_labels->data[(int)nn->data[i12] - 1] == val);
        }

        nx = x->size[1];
        idx = 0;
        i12 = ii->size[0] * ii->size[1];
        ii->size[0] = 1;
        ii->size[1] = x->size[1];
        emxEnsureCapacity_int32_T(ii, i12);
        b_new_labels = 1;
        exitg2 = false;
        while ((!exitg2) && (b_new_labels <= nx)) {
          if (x->data[b_new_labels - 1]) {
            idx++;
            ii->data[idx - 1] = b_new_labels;
            if (idx >= nx) {
              exitg2 = true;
            } else {
              b_new_labels++;
            }
          } else {
            b_new_labels++;
          }
        }

        if (x->size[1] == 1) {
          if (idx == 0) {
            i12 = ii->size[0] * ii->size[1];
            ii->size[0] = 1;
            ii->size[1] = 0;
            emxEnsureCapacity_int32_T(ii, i12);
          }
        } else {
          i12 = ii->size[0] * ii->size[1];
          if (1 > idx) {
            ii->size[1] = 0;
          } else {
            ii->size[1] = idx;
          }

          emxEnsureCapacity_int32_T(ii, i12);
        }

        i12 = ff->size[0] * ff->size[1];
        ff->size[0] = 1;
        ff->size[1] = ii->size[1];
        emxEnsureCapacity_real_T(ff, i12);
        idx = ii->size[0] * ii->size[1];
        for (i12 = 0; i12 < idx; i12++) {
          ff->data[i12] = ii->data[i12];
        }

        val = (double)ff->size[1] * b_rand();
        vs->data[(int)o_labels->data[gg->data[rr] - 1] - 1]--;
        i12 = (int)ff->data[(int)std::ceil(val) - 1] - 1;
        o_labels->data[gg->data[rr] - 1] = nn2->data[nn2->size[0] * i12];
        vs->data[(int)o_labels->data[gg->data[rr] - 1] - 1]++;
      }
    }

    pop2_Q->data[num] = compute_Q2(new_labels, W, degrees, m, jin);
    idx = pop2->size[1];
    i11 = r4->size[0];
    r4->size[0] = idx;
    emxEnsureCapacity_int32_T1(r4, i11);
    for (i11 = 0; i11 < idx; i11++) {
      r4->data[i11] = i11;
    }

    b_new_labels = r4->size[0];
    for (i11 = 0; i11 < b_new_labels; i11++) {
      pop2->data[num + pop2->size[0] * r4->data[i11]] = o_labels->data[i11];
    }

    num++;
  }

  emxFree_real_T(&b_degrees);
  emxFree_real_T(&b_ff);
  emxFree_int32_T(&ii);
  emxFree_boolean_T(&x);
  emxFree_real_T(&difference);
  emxFree_int32_T(&r4);
  emxFree_int32_T(&gg);
  emxFree_real_T(&di);
  emxFree_real_T(&indexs);
  emxFree_real_T(&b_index);
  emxFree_real_T(&colors);
  emxFree_real_T(&nn);
  emxFree_real_T(&nn2);
  emxFree_real_T(&ff);
  emxFree_real_T(&new_labels);
  emxFree_real_T(&V);
  emxFree_real_T(&vs);
  emxFree_real_T(&o_labels);
}

/* End of code generation (Mutation.cpp) */
