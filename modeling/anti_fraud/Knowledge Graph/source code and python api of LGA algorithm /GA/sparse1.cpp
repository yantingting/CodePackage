/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * sparse1.cpp
 *
 * Code generation for function 'sparse1'
 *
 */

/* Include files */
#include "rt_nonfinite.h"
#include "GACD.h"
#include "sparse1.h"
#include "GACD_emxutil.h"

/* Function Definitions */
void assertValidIndexArg(const emxArray_real_T *s, emxArray_int32_T *sint)
{
  int k;
  k = sint->size[0];
  sint->size[0] = s->size[1];
  emxEnsureCapacity_int32_T1(sint, k);
  for (k = 0; k < s->size[1]; k++) {
    sint->data[k] = (int)s->data[k];
  }
}

void permuteVector(const emxArray_int32_T *idx, emxArray_int32_T *y)
{
  emxArray_int32_T *t;
  int ny;
  int k;
  int loop_ub;
  emxInit_int32_T(&t, 1);
  ny = y->size[0];
  k = t->size[0];
  t->size[0] = y->size[0];
  emxEnsureCapacity_int32_T1(t, k);
  loop_ub = y->size[0];
  for (k = 0; k < loop_ub; k++) {
    t->data[k] = y->data[k];
  }

  for (k = 0; k < ny; k++) {
    y->data[k] = t->data[idx->data[k] - 1];
  }

  emxFree_int32_T(&t);
}

/* End of code generation (sparse1.cpp) */
