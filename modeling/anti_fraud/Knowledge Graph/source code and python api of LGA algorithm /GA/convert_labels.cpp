/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * convert_labels.cpp
 *
 * Code generation for function 'convert_labels'
 *
 */

/* Include files */
#include "rt_nonfinite.h"
#include "GACD.h"
#include "convert_labels.h"
#include "GACD_emxutil.h"
#include "diff.h"
#include "sparse.h"

/* Function Definitions */
void convert_labels(const emxArray_real_T *g, emxArray_real_T *labels)
{
  emxArray_real_T *x;
  int i2;
  int col;
  emxArray_real_T *b_x;
  emxArray_real_T *v;
  emxArray_real_T *r1;
  emxArray_int32_T *W_colidx;
  coder_internal_sparse expl_temp;
  emxArray_int32_T *W_rowidx;
  int nx;
  emxArray_int32_T *i;
  emxArray_int32_T *j;
  int idx;
  emxArray_int32_T *r;
  emxArray_int32_T *ii;
  boolean_T exitg1;
  double count;
  emxArray_int32_T *s;
  unsigned int sp;
  double b_j;
  emxInit_real_T(&x, 2);
  if (g->size[1] < 1) {
    i2 = x->size[0] * x->size[1];
    x->size[0] = 1;
    x->size[1] = 0;
    emxEnsureCapacity_real_T(x, i2);
  } else {
    i2 = g->size[1];
    col = x->size[0] * x->size[1];
    x->size[0] = 1;
    x->size[1] = (int)((double)i2 - 1.0) + 1;
    emxEnsureCapacity_real_T(x, col);
    col = (int)((double)i2 - 1.0);
    for (i2 = 0; i2 <= col; i2++) {
      x->data[x->size[0] * i2] = 1.0 + (double)i2;
    }
  }

  emxInit_real_T(&b_x, 2);
  i2 = b_x->size[0] * b_x->size[1];
  b_x->size[0] = 1;
  b_x->size[1] = x->size[1] + g->size[1];
  emxEnsureCapacity_real_T(b_x, i2);
  col = x->size[1];
  for (i2 = 0; i2 < col; i2++) {
    b_x->data[b_x->size[0] * i2] = x->data[x->size[0] * i2];
  }

  col = g->size[1];
  for (i2 = 0; i2 < col; i2++) {
    b_x->data[b_x->size[0] * (i2 + x->size[1])] = g->data[g->size[0] * i2];
  }

  emxInit_real_T(&v, 2);
  i2 = v->size[0] * v->size[1];
  v->size[0] = 1;
  v->size[1] = g->size[1] + x->size[1];
  emxEnsureCapacity_real_T(v, i2);
  col = g->size[1];
  for (i2 = 0; i2 < col; i2++) {
    v->data[v->size[0] * i2] = g->data[g->size[0] * i2];
  }

  col = x->size[1];
  for (i2 = 0; i2 < col; i2++) {
    v->data[v->size[0] * (i2 + g->size[1])] = x->data[x->size[0] * i2];
  }

  emxInit_real_T(&r1, 2);
  i2 = r1->size[0] * r1->size[1];
  r1->size[0] = 1;
  r1->size[1] = (int)(2.0 * (double)g->size[1]);
  emxEnsureCapacity_real_T(r1, i2);
  col = (int)(2.0 * (double)g->size[1]);
  for (i2 = 0; i2 < col; i2++) {
    r1->data[i2] = 1.0;
  }

  emxInit_int32_T(&W_colidx, 1);
  c_emxInitStruct_coder_internal_(&expl_temp);
  sparse(b_x, v, &expl_temp);
  i2 = W_colidx->size[0];
  W_colidx->size[0] = expl_temp.colidx->size[0];
  emxEnsureCapacity_int32_T1(W_colidx, i2);
  col = expl_temp.colidx->size[0];
  emxFree_real_T(&r1);
  for (i2 = 0; i2 < col; i2++) {
    W_colidx->data[i2] = expl_temp.colidx->data[i2];
  }

  emxInit_int32_T(&W_rowidx, 1);
  i2 = W_rowidx->size[0];
  W_rowidx->size[0] = expl_temp.rowidx->size[0];
  emxEnsureCapacity_int32_T1(W_rowidx, i2);
  col = expl_temp.rowidx->size[0];
  for (i2 = 0; i2 < col; i2++) {
    W_rowidx->data[i2] = expl_temp.rowidx->data[i2];
  }

  c_emxFreeStruct_coder_internal_(&expl_temp);
  nx = W_colidx->data[W_colidx->size[0] - 1] - 1;
  emxInit_int32_T(&i, 1);
  emxInit_int32_T(&j, 1);
  if (W_colidx->data[W_colidx->size[0] - 1] - 1 == 0) {
    i2 = i->size[0];
    i->size[0] = 0;
    emxEnsureCapacity_int32_T1(i, i2);
    i2 = j->size[0];
    j->size[0] = 0;
    emxEnsureCapacity_int32_T1(j, i2);
  } else {
    i2 = i->size[0];
    i->size[0] = W_colidx->data[W_colidx->size[0] - 1] - 1;
    emxEnsureCapacity_int32_T1(i, i2);
    i2 = j->size[0];
    j->size[0] = W_colidx->data[W_colidx->size[0] - 1] - 1;
    emxEnsureCapacity_int32_T1(j, i2);
    for (idx = 0; idx < nx; idx++) {
      i->data[idx] = W_rowidx->data[idx];
    }

    idx = 0;
    col = 1;
    while (idx < nx) {
      if (idx == W_colidx->data[col] - 1) {
        col++;
      } else {
        idx++;
        j->data[idx - 1] = col;
      }
    }

    if (W_colidx->data[W_colidx->size[0] - 1] - 1 == 1) {
      if (idx == 0) {
        i2 = i->size[0];
        i->size[0] = 0;
        emxEnsureCapacity_int32_T1(i, i2);
        i2 = j->size[0];
        j->size[0] = 0;
        emxEnsureCapacity_int32_T1(j, i2);
      }
    } else {
      i2 = i->size[0];
      if (1 > idx) {
        i->size[0] = 0;
      } else {
        i->size[0] = idx;
      }

      emxEnsureCapacity_int32_T1(i, i2);
      i2 = j->size[0];
      if (1 > idx) {
        j->size[0] = 0;
      } else {
        j->size[0] = idx;
      }

      emxEnsureCapacity_int32_T1(j, i2);
    }
  }

  emxFree_int32_T(&W_rowidx);
  emxFree_int32_T(&W_colidx);
  emxInit_int32_T1(&r, 2);
  i2 = r->size[0] * r->size[1];
  r->size[0] = 1;
  r->size[1] = i->size[0];
  emxEnsureCapacity_int32_T(r, i2);
  col = i->size[0];
  for (i2 = 0; i2 < col; i2++) {
    r->data[r->size[0] * i2] = i->data[i2];
  }

  emxFree_int32_T(&i);
  i2 = b_x->size[0] * b_x->size[1];
  b_x->size[0] = 1;
  b_x->size[1] = j->size[0];
  emxEnsureCapacity_real_T(b_x, i2);
  col = j->size[0];
  for (i2 = 0; i2 < col; i2++) {
    b_x->data[b_x->size[0] * i2] = j->data[i2];
  }

  emxFree_int32_T(&j);
  diff(b_x, v);
  i2 = x->size[0] * x->size[1];
  x->size[0] = 1;
  x->size[1] = 2 + v->size[1];
  emxEnsureCapacity_real_T(x, i2);
  x->data[0] = 1.0;
  col = v->size[1];
  emxFree_real_T(&b_x);
  for (i2 = 0; i2 < col; i2++) {
    x->data[x->size[0] * (i2 + 1)] = v->data[v->size[0] * i2];
  }

  emxInit_int32_T1(&ii, 2);
  x->data[x->size[0] * (1 + v->size[1])] = 1.0;
  nx = x->size[1];
  idx = 0;
  i2 = ii->size[0] * ii->size[1];
  ii->size[0] = 1;
  ii->size[1] = x->size[1];
  emxEnsureCapacity_int32_T(ii, i2);
  col = 1;
  exitg1 = false;
  while ((!exitg1) && (col <= nx)) {
    if (x->data[col - 1] != 0.0) {
      idx++;
      ii->data[idx - 1] = col;
      if (idx >= nx) {
        exitg1 = true;
      } else {
        col++;
      }
    } else {
      col++;
    }
  }

  i2 = ii->size[0] * ii->size[1];
  if (1 > idx) {
    ii->size[1] = 0;
  } else {
    ii->size[1] = idx;
  }

  emxEnsureCapacity_int32_T(ii, i2);
  i2 = x->size[0] * x->size[1];
  x->size[0] = 1;
  x->size[1] = ii->size[1];
  emxEnsureCapacity_real_T(x, i2);
  col = ii->size[0] * ii->size[1];
  for (i2 = 0; i2 < col; i2++) {
    x->data[i2] = ii->data[i2];
  }

  emxFree_int32_T(&ii);
  count = 1.0;
  i2 = labels->size[0] * labels->size[1];
  labels->size[0] = 1;
  labels->size[1] = g->size[1];
  emxEnsureCapacity_real_T(labels, i2);
  col = g->size[1];
  for (i2 = 0; i2 < col; i2++) {
    labels->data[i2] = 0.0;
  }

  i2 = v->size[0] * v->size[1];
  v->size[0] = 1;
  v->size[1] = g->size[1];
  emxEnsureCapacity_real_T(v, i2);
  col = g->size[1];
  for (i2 = 0; i2 < col; i2++) {
    v->data[i2] = 0.0;
  }

  emxInit_int32_T1(&s, 2);
  i2 = s->size[0] * s->size[1];
  s->size[0] = 1;
  s->size[1] = g->size[1];
  emxEnsureCapacity_int32_T(s, i2);
  col = g->size[1];
  for (i2 = 0; i2 < col; i2++) {
    s->data[i2] = 0;
  }

  for (nx = 0; nx < g->size[1]; nx++) {
    if ((signed char)v->data[nx] != 1) {
      s->data[0] = 1 + nx;
      sp = 2U;
      while (sp != 1U) {
        sp--;
        labels->data[s->data[(int)sp - 1] - 1] = count;
        v->data[s->data[(int)sp - 1] - 1] = 1.0;
        i2 = (int)x->data[s->data[(int)sp - 1] - 1];
        col = (int)(((double)(int)x->data[s->data[(int)sp - 1]] - 1.0) + (1.0 -
          (double)(int)x->data[s->data[(int)sp - 1] - 1]));
        for (idx = 0; idx < col; idx++) {
          b_j = (double)i2 + (double)idx;
          if ((signed char)v->data[r->data[(int)b_j - 1] - 1] != 1) {
            s->data[(int)sp - 1] = r->data[(int)b_j - 1];
            sp++;
          }
        }
      }

      count++;
    }
  }

  emxFree_real_T(&v);
  emxFree_int32_T(&r);
  emxFree_int32_T(&s);
  emxFree_real_T(&x);
}

/* End of code generation (convert_labels.cpp) */
