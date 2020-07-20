/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * compute_Q2.cpp
 *
 * Code generation for function 'compute_Q2'
 *
 */

/* Include files */
#include "rt_nonfinite.h"
#include "GACD.h"
#include "compute_Q2.h"
#include "GACD_emxutil.h"
#include "sum.h"
#include "find_communities.h"

/* Function Definitions */
double compute_Q2(const emxArray_real_T *labels, const emxArray_real_T *W, const
                  emxArray_real_T *degrees, double m, const emxArray_real_T *jin)
{
  double Q;
  emxArray_real_T *V;
  emxArray_real_T *indexs;
  emxArray_real_T *di;
  int i4;
  int loop_ub;
  int i;
  emxArray_real_T *b_index;
  emxArray_real_T *nn2;
  emxArray_int32_T *r2;
  emxArray_real_T *b_V;
  emxArray_real_T *b_degrees;
  int i5;
  int i6;
  int j;
  double id;
  int i7;
  int i8;
  emxInit_real_T(&V, 2);
  emxInit_real_T(&indexs, 2);
  emxInit_real_T(&di, 2);
  Q = 0.0;
  find_communities(labels, indexs, di);
  i4 = V->size[0] * V->size[1];
  V->size[0] = 1;
  V->size[1] = labels->size[1];
  emxEnsureCapacity_real_T(V, i4);
  loop_ub = labels->size[1];
  for (i4 = 0; i4 < loop_ub; i4++) {
    V->data[i4] = 0.0;
  }

  i = 0;
  emxInit_real_T(&b_index, 2);
  emxInit_real_T(&nn2, 2);
  emxInit_int32_T1(&r2, 2);
  emxInit_real_T(&b_V, 2);
  emxInit_real_T(&b_degrees, 2);
  while (i <= di->size[1] - 2) {
    if (di->data[i] > di->data[i + 1] - 1.0) {
      i4 = 1;
      i5 = 1;
    } else {
      i4 = (int)di->data[i];
      i5 = (int)(di->data[i + 1] - 1.0) + 1;
    }

    i6 = b_index->size[0] * b_index->size[1];
    b_index->size[0] = 1;
    b_index->size[1] = i5 - i4;
    emxEnsureCapacity_real_T(b_index, i6);
    loop_ub = i5 - i4;
    for (i6 = 0; i6 < loop_ub; i6++) {
      b_index->data[b_index->size[0] * i6] = indexs->data[(i4 + i6) - 1];
    }

    for (j = -1; j < (i5 - i4) - 1; j++) {
      id = indexs->data[i4 + j];
      if (jin->data[(int)id - 1] > jin->data[(int)(id + 1.0) - 1] - 1.0) {
        i6 = 0;
        i7 = 0;
      } else {
        i6 = (int)jin->data[(int)id - 1] - 1;
        i7 = (int)(jin->data[(int)(id + 1.0) - 1] - 1.0);
      }

      i8 = nn2->size[0] * nn2->size[1];
      nn2->size[0] = 2;
      nn2->size[1] = i7 - i6;
      emxEnsureCapacity_real_T(nn2, i8);
      loop_ub = i7 - i6;
      for (i7 = 0; i7 < loop_ub; i7++) {
        for (i8 = 0; i8 < 2; i8++) {
          nn2->data[i8 + nn2->size[0] * i7] = W->data[(i6 + i7) + W->size[0] *
            (i8 << 1)];
        }
      }

      loop_ub = nn2->size[1];
      i6 = r2->size[0] * r2->size[1];
      r2->size[0] = 1;
      r2->size[1] = loop_ub;
      emxEnsureCapacity_int32_T(r2, i6);
      for (i6 = 0; i6 < loop_ub; i6++) {
        r2->data[r2->size[0] * i6] = (int)nn2->data[nn2->size[0] * i6];
      }

      loop_ub = nn2->size[1] - 1;
      for (i6 = 0; i6 <= loop_ub; i6++) {
        V->data[r2->data[r2->size[0] * i6] - 1] = nn2->data[1 + nn2->size[0] *
          i6];
      }

      i6 = b_V->size[0] * b_V->size[1];
      b_V->size[0] = 1;
      b_V->size[1] = b_index->size[1];
      emxEnsureCapacity_real_T(b_V, i6);
      loop_ub = b_index->size[0] * b_index->size[1];
      for (i6 = 0; i6 < loop_ub; i6++) {
        b_V->data[i6] = V->data[(int)b_index->data[i6] - 1];
      }

      i6 = b_degrees->size[0] * b_degrees->size[1];
      b_degrees->size[0] = 1;
      b_degrees->size[1] = b_index->size[1];
      emxEnsureCapacity_real_T(b_degrees, i6);
      id = degrees->data[(int)indexs->data[i4 + j] - 1];
      loop_ub = b_index->size[0] * b_index->size[1];
      for (i6 = 0; i6 < loop_ub; i6++) {
        b_degrees->data[i6] = id * degrees->data[(int)b_index->data[i6] - 1];
      }

      Q += b_sum(b_V) - b_sum(b_degrees) / (2.0 * m);
      loop_ub = nn2->size[1];
      i6 = r2->size[0] * r2->size[1];
      r2->size[0] = 1;
      r2->size[1] = loop_ub;
      emxEnsureCapacity_int32_T(r2, i6);
      for (i6 = 0; i6 < loop_ub; i6++) {
        r2->data[r2->size[0] * i6] = (int)nn2->data[nn2->size[0] * i6];
      }

      loop_ub = r2->size[0] * r2->size[1] - 1;
      for (i6 = 0; i6 <= loop_ub; i6++) {
        V->data[r2->data[i6] - 1] = 0.0;
      }
    }

    i++;
  }

  emxFree_real_T(&b_degrees);
  emxFree_real_T(&b_V);
  emxFree_int32_T(&r2);
  emxFree_real_T(&di);
  emxFree_real_T(&indexs);
  emxFree_real_T(&nn2);
  emxFree_real_T(&b_index);
  emxFree_real_T(&V);
  Q /= 2.0 * m;
  return Q;
}

/* End of code generation (compute_Q2.cpp) */
