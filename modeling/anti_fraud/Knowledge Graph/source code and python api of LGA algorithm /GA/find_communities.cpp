/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * find_communities.cpp
 *
 * Code generation for function 'find_communities'
 *
 */

/* Include files */
#include "rt_nonfinite.h"
#include "GACD.h"
#include "find_communities.h"
#include "GACD_emxutil.h"
#include "diff.h"
#include "sort1.h"

/* Function Definitions */
void find_communities(const emxArray_real_T *labels, emxArray_real_T *indexs,
                      emxArray_real_T *di)
{
  emxArray_real_T *v;
  int i9;
  int loop_ub;
  emxArray_int32_T *ii;
  emxArray_real_T *r3;
  int nx;
  int idx;
  boolean_T exitg1;
  emxInit_real_T(&v, 2);
  i9 = v->size[0] * v->size[1];
  v->size[0] = 1;
  v->size[1] = labels->size[1];
  emxEnsureCapacity_real_T(v, i9);
  loop_ub = labels->size[0] * labels->size[1];
  for (i9 = 0; i9 < loop_ub; i9++) {
    v->data[i9] = labels->data[i9];
  }

  emxInit_int32_T1(&ii, 2);
  sort(v, ii);
  i9 = indexs->size[0] * indexs->size[1];
  indexs->size[0] = 1;
  indexs->size[1] = ii->size[1];
  emxEnsureCapacity_real_T(indexs, i9);
  loop_ub = ii->size[0] * ii->size[1];
  for (i9 = 0; i9 < loop_ub; i9++) {
    indexs->data[i9] = ii->data[i9];
  }

  emxInit_real_T(&r3, 2);
  diff(v, r3);
  i9 = v->size[0] * v->size[1];
  v->size[0] = 1;
  v->size[1] = 2 + r3->size[1];
  emxEnsureCapacity_real_T(v, i9);
  v->data[0] = 1.0;
  loop_ub = r3->size[1];
  for (i9 = 0; i9 < loop_ub; i9++) {
    v->data[v->size[0] * (i9 + 1)] = r3->data[r3->size[0] * i9];
  }

  v->data[v->size[0] * (1 + r3->size[1])] = 1.0;
  nx = v->size[1];
  idx = 0;
  i9 = ii->size[0] * ii->size[1];
  ii->size[0] = 1;
  ii->size[1] = v->size[1];
  emxEnsureCapacity_int32_T(ii, i9);
  loop_ub = 1;
  emxFree_real_T(&r3);
  exitg1 = false;
  while ((!exitg1) && (loop_ub <= nx)) {
    if (v->data[loop_ub - 1] != 0.0) {
      idx++;
      ii->data[idx - 1] = loop_ub;
      if (idx >= nx) {
        exitg1 = true;
      } else {
        loop_ub++;
      }
    } else {
      loop_ub++;
    }
  }

  emxFree_real_T(&v);
  i9 = ii->size[0] * ii->size[1];
  if (1 > idx) {
    ii->size[1] = 0;
  } else {
    ii->size[1] = idx;
  }

  emxEnsureCapacity_int32_T(ii, i9);
  i9 = di->size[0] * di->size[1];
  di->size[0] = 1;
  di->size[1] = ii->size[1];
  emxEnsureCapacity_real_T(di, i9);
  loop_ub = ii->size[0] * ii->size[1];
  for (i9 = 0; i9 < loop_ub; i9++) {
    di->data[i9] = ii->data[i9];
  }

  emxFree_int32_T(&ii);
}

/* End of code generation (find_communities.cpp) */
