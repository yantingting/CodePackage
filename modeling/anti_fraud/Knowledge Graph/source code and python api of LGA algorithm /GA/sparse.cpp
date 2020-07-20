/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * sparse.cpp
 *
 * Code generation for function 'sparse'
 *
 */

/* Include files */
#include "rt_nonfinite.h"
#include "GACD.h"
#include "sparse.h"
#include "GACD_emxutil.h"
#include "sparse1.h"
#include "introsort.h"

/* Function Definitions */
void sparse(const emxArray_real_T *varargin_1, const emxArray_real_T *varargin_2,
            coder_internal_sparse *y)
{
  emxArray_int32_T *ridxInt;
  emxArray_int32_T *cidxInt;
  emxArray_int32_T *sortedIndices;
  int nc;
  int i3;
  int numalloc;
  cell_wrap_1 tunableEnvironment[2];
  cell_wrap_1 this_tunableEnvironment[2];
  int thism;
  int thisn;
  int c;
  double val;
  emxInit_int32_T(&ridxInt, 1);
  emxInit_int32_T(&cidxInt, 1);
  emxInit_int32_T(&sortedIndices, 1);
  nc = varargin_2->size[1];
  assertValidIndexArg(varargin_1, ridxInt);
  assertValidIndexArg(varargin_2, cidxInt);
  i3 = sortedIndices->size[0];
  sortedIndices->size[0] = varargin_2->size[1];
  emxEnsureCapacity_int32_T1(sortedIndices, i3);
  for (numalloc = 1; numalloc <= nc; numalloc++) {
    sortedIndices->data[numalloc - 1] = numalloc;
  }

  emxInitMatrix_cell_wrap_1(tunableEnvironment);
  i3 = tunableEnvironment[0].f1->size[0];
  tunableEnvironment[0].f1->size[0] = cidxInt->size[0];
  emxEnsureCapacity_int32_T1(tunableEnvironment[0].f1, i3);
  numalloc = cidxInt->size[0];
  for (i3 = 0; i3 < numalloc; i3++) {
    tunableEnvironment[0].f1->data[i3] = cidxInt->data[i3];
  }

  i3 = tunableEnvironment[1].f1->size[0];
  tunableEnvironment[1].f1->size[0] = ridxInt->size[0];
  emxEnsureCapacity_int32_T1(tunableEnvironment[1].f1, i3);
  numalloc = ridxInt->size[0];
  for (i3 = 0; i3 < numalloc; i3++) {
    tunableEnvironment[1].f1->data[i3] = ridxInt->data[i3];
  }

  emxInitMatrix_cell_wrap_1(this_tunableEnvironment);
  for (i3 = 0; i3 < 2; i3++) {
    emxCopyStruct_cell_wrap_1(&this_tunableEnvironment[i3],
      &tunableEnvironment[i3]);
  }

  emxFreeMatrix_cell_wrap_1(tunableEnvironment);
  introsort(sortedIndices, cidxInt->size[0], this_tunableEnvironment);
  permuteVector(sortedIndices, cidxInt);
  permuteVector(sortedIndices, ridxInt);
  emxFreeMatrix_cell_wrap_1(this_tunableEnvironment);
  emxFree_int32_T(&sortedIndices);
  if ((ridxInt->size[0] == 0) || (cidxInt->size[0] == 0)) {
    thism = 0;
    thisn = 0;
  } else {
    thism = ridxInt->data[0];
    for (numalloc = 1; numalloc < ridxInt->size[0]; numalloc++) {
      if (thism < ridxInt->data[numalloc]) {
        thism = ridxInt->data[numalloc];
      }
    }

    thisn = cidxInt->data[cidxInt->size[0] - 1];
  }

  y->m = thism;
  y->n = thisn;
  if (varargin_2->size[1] >= 1) {
    numalloc = varargin_2->size[1];
  } else {
    numalloc = 1;
  }

  i3 = y->d->size[0];
  y->d->size[0] = numalloc;
  emxEnsureCapacity_real_T1(y->d, i3);
  for (i3 = 0; i3 < numalloc; i3++) {
    y->d->data[i3] = 0.0;
  }

  i3 = y->colidx->size[0];
  y->colidx->size[0] = thisn + 1;
  emxEnsureCapacity_int32_T1(y->colidx, i3);
  y->colidx->data[0] = 1;
  i3 = y->rowidx->size[0];
  y->rowidx->size[0] = numalloc;
  emxEnsureCapacity_int32_T1(y->rowidx, i3);
  for (i3 = 0; i3 < numalloc; i3++) {
    y->rowidx->data[i3] = 0;
  }

  thism = 0;
  for (c = 1; c <= thisn; c++) {
    while ((thism + 1 <= nc) && (cidxInt->data[thism] == c)) {
      y->rowidx->data[thism] = ridxInt->data[thism];
      thism++;
    }

    y->colidx->data[c] = thism + 1;
  }

  emxFree_int32_T(&cidxInt);
  emxFree_int32_T(&ridxInt);
  for (numalloc = 1; numalloc <= nc; numalloc++) {
    y->d->data[numalloc - 1] = 1.0;
  }

  thism = 1;
  i3 = y->colidx->size[0] - 1;
  for (c = 1; c <= i3; c++) {
    numalloc = y->colidx->data[c - 1];
    y->colidx->data[c - 1] = thism;
    while (numalloc < y->colidx->data[c]) {
      val = 0.0;
      thisn = y->rowidx->data[numalloc - 1];
      while ((numalloc < y->colidx->data[c]) && (y->rowidx->data[numalloc - 1] ==
              thisn)) {
        val += y->d->data[numalloc - 1];
        numalloc++;
      }

      if (val != 0.0) {
        y->d->data[thism - 1] = val;
        y->rowidx->data[thism - 1] = thisn;
        thism++;
      }
    }
  }

  y->colidx->data[y->colidx->size[0] - 1] = thism;
}

/* End of code generation (sparse.cpp) */
