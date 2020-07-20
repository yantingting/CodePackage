/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * GACD.cpp
 *
 * Code generation for function 'GACD'
 *
 */

/* Include files */
#include <cmath>
#include "rt_nonfinite.h"
#include "GACD.h"
#include "sum.h"
#include "GACD_emxutil.h"
#include "rand.h"
#include "compute_Q2.h"
#include "convert_labels.h"
#include "sort1.h"
#include "Mutation.h"
#include "diff.h"


/* Function Definitions */
void GACD(const emxArray_real_T *W, double pop_size, double L, double p1,
          emxArray_real_T *labels, double *Q, emxArray_real_T *x, emxArray_real_T *y)
{
  emxArray_real_T *pop2;
  int i0;
  int idx;
  emxArray_real_T *pop2_Q;
  emxArray_real_T *b_W;
  emxArray_real_T *t1;
  emxArray_int32_T *ii;
  int nx;
  int b_ii;
  boolean_T exitg1;
  emxArray_real_T *jin;
  emxArray_real_T *degrees;
  emxArray_real_T *c_W;
  double m;
  int i1;
  emxArray_real_T *pop;
  emxArray_real_T *pop_Q;
  emxArray_int32_T *r0;
  double n2;
  int t;
  emxArray_real_T *v1;
  emxArray_real_T *v2;
  emxArray_int32_T *ix;
  emxArray_real_T *t3;
  emxArray_real_T *t2;
  unsigned int count;
  double pp;
  boolean_T empty_non_axis_sizes;
  emxInit_real_T(&pop2, 2);

  /* %%%%%%%%%全局变量 */
  /* %%%%%%%%%变量初始化 */
  i0 = pop2->size[0] * pop2->size[1];
  pop2->size[0] = (int)pop_size;
  pop2->size[1] = (int)W->data[(W->size[0] + W->size[0]) - 1];
  emxEnsureCapacity_real_T(pop2, i0);
  idx = (int)pop_size * (int)W->data[(W->size[0] + W->size[0]) - 1];
  for (i0 = 0; i0 < idx; i0++) {
    pop2->data[i0] = 0.0;
  }

  emxInit_real_T(&pop2_Q, 2);
  i0 = pop2_Q->size[0] * pop2_Q->size[1];
  pop2_Q->size[0] = 1;
  pop2_Q->size[1] = (int)pop_size;
  emxEnsureCapacity_real_T(pop2_Q, i0);
  idx = (int)pop_size;
  for (i0 = 0; i0 < idx; i0++) {
    pop2_Q->data[i0] = 0.0;
  }

  emxInit_real_T(&b_W, 2);
  idx = W->size[0];
  i0 = b_W->size[0] * b_W->size[1];
  b_W->size[0] = 1;
  b_W->size[1] = idx;
  emxEnsureCapacity_real_T(b_W, i0);
  for (i0 = 0; i0 < idx; i0++) {
    b_W->data[b_W->size[0] * i0] = W->data[i0 + W->size[0]];
  }

  emxInit_real_T(&t1, 2);
  diff(b_W, t1);
  i0 = x->size[0] * x->size[1];
  x->size[0] = 1;
  x->size[1] = 2 + t1->size[1];
  emxEnsureCapacity_real_T(x, i0);
  x->data[0] = 1.0;
  idx = t1->size[1];
  for (i0 = 0; i0 < idx; i0++) {
    x->data[x->size[0] * (i0 + 1)] = t1->data[t1->size[0] * i0];
  }

  emxInit_int32_T1(&ii, 2);
  x->data[x->size[0] * (1 + t1->size[1])] = 1.0;
  nx = x->size[1];
  idx = 0;
  i0 = ii->size[0] * ii->size[1];
  ii->size[0] = 1;
  ii->size[1] = x->size[1];
  emxEnsureCapacity_int32_T(ii, i0);
  b_ii = 1;
  exitg1 = false;
  while ((!exitg1) && (b_ii <= nx)) {
    if (x->data[b_ii - 1] != 0.0) {
      idx++;
      ii->data[idx - 1] = b_ii;
      if (idx >= nx) {
        exitg1 = true;
      } else {
        b_ii++;
      }
    } else {
      b_ii++;
    }
  }

  emxInit_real_T(&jin, 2);
  i0 = ii->size[0] * ii->size[1];
  if (1 > idx) {
    ii->size[1] = 0;
  } else {
    ii->size[1] = idx;
  }

  emxEnsureCapacity_int32_T(ii, i0);
  i0 = jin->size[0] * jin->size[1];
  jin->size[0] = 1;
  jin->size[1] = ii->size[1];
  emxEnsureCapacity_real_T(jin, i0);
  idx = ii->size[0] * ii->size[1];
  for (i0 = 0; i0 < idx; i0++) {
    jin->data[i0] = ii->data[i0];
  }

  emxInit_real_T(&degrees, 2);
  i0 = degrees->size[0] * degrees->size[1];
  degrees->size[0] = 1;
  degrees->size[1] = (int)W->data[(W->size[0] + W->size[0]) - 1];
  emxEnsureCapacity_real_T(degrees, i0);
  idx = (int)W->data[(W->size[0] + W->size[0]) - 1];
  for (i0 = 0; i0 < idx; i0++) {
    degrees->data[i0] = 0.0;
  }

  nx = 0;
  emxInit_real_T1(&c_W, 1);
  while (nx <= (int)((double)jin->size[1] - 1.0) - 1) {
    if ((int)jin->data[nx] > (double)(int)jin->data[(int)((1.0 + (double)nx) +
         1.0) - 1] - 1.0) {
      i0 = 0;
      i1 = 0;
    } else {
      i0 = (int)jin->data[nx] - 1;
      i1 = (int)((double)(int)jin->data[(int)((1.0 + (double)nx) + 1.0) - 1] -
                 1.0);
    }

    b_ii = c_W->size[0];
    c_W->size[0] = i1 - i0;
    emxEnsureCapacity_real_T1(c_W, b_ii);
    idx = i1 - i0;
    for (i1 = 0; i1 < idx; i1++) {
      c_W->data[i1] = W->data[(i0 + i1) + (W->size[0] << 1)];
    }

    degrees->data[nx] = sum(c_W);
    nx++;
  }

  emxFree_real_T(&c_W);
  m = b_sum(degrees) / 2.0;

  /* %%%%%%%%%变量初始化 */
  /* %%%%%%%%%%%种群初始化 */
  i0 = labels->size[0] * labels->size[1];
  labels->size[0] = 1;
  labels->size[1] = (int)W->data[(W->size[0] + W->size[0]) - 1];
  emxEnsureCapacity_real_T(labels, i0);
  idx = (int)W->data[(W->size[0] + W->size[0]) - 1];
  for (i0 = 0; i0 < idx; i0++) {
    labels->data[i0] = 0.0;
  }

  emxInit_real_T(&pop, 2);
  emxInit_real_T(&pop_Q, 2);
  i0 = pop->size[0] * pop->size[1];
  pop->size[0] = (int)pop_size;
  pop->size[1] = (int)W->data[(W->size[0] + W->size[0]) - 1];
  emxEnsureCapacity_real_T(pop, i0);
  i0 = pop_Q->size[0] * pop_Q->size[1];
  pop_Q->size[0] = 1;
  pop_Q->size[1] = (int)pop_size;
  emxEnsureCapacity_real_T(pop_Q, i0);
  nx = 0;
  emxInit_int32_T(&r0, 1);
  while (nx <= (int)pop_size - 1) {
    for (b_ii = 0; b_ii < (int)W->data[(W->size[0] + W->size[0]) - 1]; b_ii++) {
      if ((int)jin->data[b_ii] > (double)(int)jin->data[(int)((1.0 + (double)
            b_ii) + 1.0) - 1] - 1.0) {
        i0 = -1;
        i1 = -1;
      } else {
        i0 = (int)jin->data[b_ii] - 2;
        i1 = (int)((double)(int)jin->data[(int)((1.0 + (double)b_ii) + 1.0) - 1]
                   - 1.0) - 1;
      }

      n2 = (double)(i1 - i0) * b_rand();
      labels->data[b_ii] = W->data[i0 + (int)std::ceil(n2)];
    }

    idx = pop->size[1];
    i0 = r0->size[0];
    r0->size[0] = idx;
    emxEnsureCapacity_int32_T1(r0, i0);
    for (i0 = 0; i0 < idx; i0++) {
      r0->data[i0] = i0;
    }

    b_ii = r0->size[0];
    for (i0 = 0; i0 < b_ii; i0++) {
      pop->data[nx + pop->size[0] * r0->data[i0]] = labels->data[i0];
    }

    convert_labels(labels, t1);
    pop_Q->data[nx] = compute_Q2(t1, W, degrees, m, jin);
    nx++;
  }

  emxFree_int32_T(&r0);

  /* %%%%%%%%%%%种群初始化完毕 */
  /* %%%%%%%%%%%进化 */
  if (rtIsNaN(L)) {
    i0 = x->size[0] * x->size[1];
    x->size[0] = 1;
    x->size[1] = 1;
    emxEnsureCapacity_real_T(x, i0);
    x->data[0] = rtNaN;
  } else if (L < 1.0) {
    i0 = x->size[0] * x->size[1];
    x->size[0] = 1;
    x->size[1] = 0;
    emxEnsureCapacity_real_T(x, i0);
  } else if (rtIsInf(L) && (1.0 == L)) {
    i0 = x->size[0] * x->size[1];
    x->size[0] = 1;
    x->size[1] = 1;
    emxEnsureCapacity_real_T(x, i0);
    x->data[0] = rtNaN;
  } else {
    i0 = x->size[0] * x->size[1];
    x->size[0] = 1;
    x->size[1] = (int)std::floor(L - 1.0) + 1;
    emxEnsureCapacity_real_T(x, i0);
    idx = (int)std::floor(L - 1.0);
    for (i0 = 0; i0 <= idx; i0++) {
      x->data[x->size[0] * i0] = 1.0 + (double)i0;
    }
  }

  i0 = y->size[0] * y->size[1];
  y->size[0] = 1;
  y->size[1] = (int)L;
  emxEnsureCapacity_real_T(y, i0);
  t = 0;
  emxInit_real_T(&v1, 2);
  emxInit_real_T(&v2, 2);
  emxInit_int32_T1(&ix, 2);
  emxInit_real_T(&t3, 2);
  emxInit_real_T(&t2, 2);
  while (t <= (int)L - 1) {
    n2 = (double)pop->size[0] / 2.0;
    count = 0U;
    c_rand((double)pop->size[0], t1);
    sort(t1, ii);
    i0 = ix->size[0] * ix->size[1];
    ix->size[0] = 1;
    ix->size[1] = ii->size[1];
    emxEnsureCapacity_int32_T(ix, i0);
    idx = ii->size[0] * ii->size[1];
    for (i0 = 0; i0 < idx; i0++) {
      ix->data[i0] = ii->data[i0];
    }

    for (nx = 1; nx - 1 < (int)n2; nx++) {
      pp = b_rand();
      if (pp < p1) {
        c_rand((double)pop->size[1], t1);
        i0 = t1->size[1];
        for (b_ii = 0; b_ii < i0; b_ii++) {
          t1->data[b_ii] = std::floor(t1->data[b_ii] * 2.0);
        }

        i0 = t3->size[0] * t3->size[1];
        t3->size[0] = 1;
        t3->size[1] = t1->size[1];
        emxEnsureCapacity_real_T(t3, i0);
        idx = t1->size[0] * t1->size[1];
        for (i0 = 0; i0 < idx; i0++) {
          t3->data[i0] = -t1->data[i0] + 1.0;
        }

        idx = pop->size[1];
        b_ii = ix->data[(int)((unsigned int)nx << 1) - 2];
        i0 = v1->size[0] * v1->size[1];
        v1->size[0] = 1;
        v1->size[1] = idx;
        emxEnsureCapacity_real_T(v1, i0);
        for (i0 = 0; i0 < idx; i0++) {
          v1->data[v1->size[0] * i0] = pop->data[(b_ii + pop->size[0] * i0) - 1];
        }

        idx = pop->size[1];
        b_ii = ix->data[(int)((unsigned int)nx << 1) - 1];
        i0 = v2->size[0] * v2->size[1];
        v2->size[0] = 1;
        v2->size[1] = idx;
        emxEnsureCapacity_real_T(v2, i0);
        for (i0 = 0; i0 < idx; i0++) {
          v2->data[v2->size[0] * i0] = pop->data[(b_ii + pop->size[0] * i0) - 1];
        }

        count += 2U;
        idx = v1->size[1];
        for (i0 = 0; i0 < idx; i0++) {
          pop2->data[((int)count + pop2->size[0] * i0) - 2] = v1->data[v1->size
            [0] * i0] * t1->data[t1->size[0] * i0] + v2->data[v2->size[0] * i0] *
            t3->data[t3->size[0] * i0];
        }

        idx = v1->size[1];
        for (i0 = 0; i0 < idx; i0++) {
          pop2->data[((int)count + pop2->size[0] * i0) - 1] = v1->data[v1->size
            [0] * i0] * t3->data[t3->size[0] * i0] + v2->data[v2->size[0] * i0] *
            t1->data[t1->size[0] * i0];
        }
      }
    }

    Mutation(W, degrees, m, jin, pop2, pop2_Q, (double)count);
    i0 = t3->size[0] * t3->size[1];
    t3->size[0] = 1;
    t3->size[1] = pop_Q->size[1] + pop2_Q->size[1];
    emxEnsureCapacity_real_T(t3, i0);
    idx = pop_Q->size[1];
    for (i0 = 0; i0 < idx; i0++) {
      t3->data[t3->size[0] * i0] = pop_Q->data[pop_Q->size[0] * i0];
    }

    idx = pop2_Q->size[1];
    for (i0 = 0; i0 < idx; i0++) {
      t3->data[t3->size[0] * (i0 + pop_Q->size[1])] = pop2_Q->data[pop2_Q->size
        [0] * i0];
    }

    i0 = t1->size[0] * t1->size[1];
    t1->size[0] = 1;
    t1->size[1] = t3->size[1];
    emxEnsureCapacity_real_T(t1, i0);
    idx = t3->size[0] * t3->size[1];
    for (i0 = 0; i0 < idx; i0++) {
      t1->data[i0] = t3->data[i0];
    }

    c_sort(t1, ii);
    i0 = ix->size[0] * ix->size[1];
    ix->size[0] = 1;
    ix->size[1] = ii->size[1];
    emxEnsureCapacity_int32_T(ix, i0);
    idx = ii->size[0] * ii->size[1];
    for (i0 = 0; i0 < idx; i0++) {
      ix->data[i0] = ii->data[i0];
    }

    if (!((pop->size[0] == 0) || (pop->size[1] == 0))) {
      b_ii = pop->size[1];
    } else if (!((pop2->size[0] == 0) || (pop2->size[1] == 0))) {
      b_ii = pop2->size[1];
    } else {
      b_ii = pop->size[1];
      if (!(b_ii > 0)) {
        b_ii = 0;
      }

      if (pop2->size[1] > b_ii) {
        b_ii = pop2->size[1];
      }
    }

    empty_non_axis_sizes = (b_ii == 0);
    if (empty_non_axis_sizes || (!((pop->size[0] == 0) || (pop->size[1] == 0))))
    {
      nx = pop->size[0];
    } else {
      nx = 0;
    }

    if (empty_non_axis_sizes || (!((pop2->size[0] == 0) || (pop2->size[1] == 0))))
    {
      idx = pop2->size[0];
    } else {
      idx = 0;
    }

    i0 = t2->size[0] * t2->size[1];
    t2->size[0] = nx + idx;
    t2->size[1] = b_ii;
    emxEnsureCapacity_real_T(t2, i0);
    for (i0 = 0; i0 < b_ii; i0++) {
      for (i1 = 0; i1 < nx; i1++) {
        t2->data[i1 + t2->size[0] * i0] = pop->data[i1 + nx * i0];
      }
    }

    for (i0 = 0; i0 < b_ii; i0++) {
      for (i1 = 0; i1 < idx; i1++) {
        t2->data[(i1 + nx) + t2->size[0] * i0] = pop2->data[i1 + idx * i0];
      }
    }

    if (1 > pop->size[0]) {
      idx = 0;
    } else {
      idx = pop->size[0];
    }

    b_ii = t2->size[1];
    i0 = pop->size[0] * pop->size[1];
    pop->size[0] = idx;
    pop->size[1] = b_ii;
    emxEnsureCapacity_real_T(pop, i0);
    for (i0 = 0; i0 < b_ii; i0++) {
      for (i1 = 0; i1 < idx; i1++) {
        pop->data[i1 + pop->size[0] * i0] = t2->data[(ix->data[i1] + t2->size[0]
          * i0) - 1];
      }
    }

    if (1 > pop_Q->size[1]) {
      idx = 0;
    } else {
      idx = pop_Q->size[1];
    }

    i0 = pop_Q->size[0] * pop_Q->size[1];
    pop_Q->size[0] = 1;
    pop_Q->size[1] = idx;
    emxEnsureCapacity_real_T(pop_Q, i0);
    for (i0 = 0; i0 < idx; i0++) {
      pop_Q->data[pop_Q->size[0] * i0] = t3->data[ix->data[i0] - 1];
    }

    b_ii = pop2->size[0];
    nx = pop2->size[1];
    for (i0 = 0; i0 < nx; i0++) {
      for (i1 = 0; i1 < b_ii; i1++) {
        pop2->data[i1 + pop2->size[0] * i0] = 0.0;
      }
    }

    i0 = pop2_Q->size[0] * pop2_Q->size[1];
    pop2_Q->size[0] = 1;
    emxEnsureCapacity_real_T(pop2_Q, i0);
    b_ii = pop2_Q->size[1];
    for (i0 = 0; i0 < b_ii; i0++) {
      pop2_Q->data[pop2_Q->size[0] * i0] = 0.0;
    }

    i0 = ii->size[0] * ii->size[1];
    ii->size[0] = 1;
    ii->size[1] = idx;
    emxEnsureCapacity_int32_T(ii, i0);
    for (i0 = 0; i0 < idx; i0++) {
      ii->data[ii->size[0] * i0] = ix->data[i0];
    }

    y->data[t] = t3->data[ii->data[0] - 1];
    t++;
  }

  emxFree_real_T(&t2);
  emxFree_real_T(&t3);
  emxFree_int32_T(&ix);
  emxFree_real_T(&v2);
  emxFree_real_T(&v1);
  emxFree_real_T(&t1);
  emxFree_int32_T(&ii);
  emxFree_real_T(&degrees);
  emxFree_real_T(&jin);
  emxFree_real_T(&pop2_Q);
  emxFree_real_T(&pop2);

  /* %%%%%%%%%%%进化 */
  idx = pop->size[1];
  i0 = b_W->size[0] * b_W->size[1];
  b_W->size[0] = 1;
  b_W->size[1] = idx;
  emxEnsureCapacity_real_T(b_W, i0);
  for (i0 = 0; i0 < idx; i0++) {
    b_W->data[b_W->size[0] * i0] = pop->data[pop->size[0] * i0];
  }

  emxFree_real_T(&pop);
  convert_labels(b_W, labels);
  *Q = pop_Q->data[0];
  emxFree_real_T(&b_W);
  emxFree_real_T(&pop_Q);
}

/* End of code generation (GACD.cpp) */
