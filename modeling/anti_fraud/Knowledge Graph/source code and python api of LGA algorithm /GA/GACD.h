/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * GACD.h
 *
 * Code generation for function 'GACD'
 *
 */

#ifndef GACD_H
#define GACD_H

/* Include files */
#include <stddef.h>
#include <stdlib.h>
#include "rtwtypes.h"
#include "GACD_types.h"

/* Function Declarations */
extern void GACD(const emxArray_real_T *W, double pop_size, double L, double p1,
                 emxArray_real_T *labels, double *Q, emxArray_real_T *x,
                 emxArray_real_T *y);

#endif

/* End of code generation (GACD.h) */
