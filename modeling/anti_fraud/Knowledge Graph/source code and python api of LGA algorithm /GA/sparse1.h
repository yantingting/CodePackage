/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * sparse1.h
 *
 * Code generation for function 'sparse1'
 *
 */

#ifndef SPARSE1_H
#define SPARSE1_H

/* Include files */
#include <stddef.h>
#include <stdlib.h>
#include "rtwtypes.h"
#include "GACD_types.h"

/* Function Declarations */
extern void assertValidIndexArg(const emxArray_real_T *s, emxArray_int32_T *sint);
extern void permuteVector(const emxArray_int32_T *idx, emxArray_int32_T *y);

#endif

/* End of code generation (sparse1.h) */
