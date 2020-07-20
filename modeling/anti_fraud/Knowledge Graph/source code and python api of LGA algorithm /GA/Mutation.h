/*
 * Academic License - for use in teaching, academic research, and meeting
 * course requirements at degree granting institutions only.  Not for
 * government, commercial, or other organizational use.
 *
 * Mutation.h
 *
 * Code generation for function 'Mutation'
 *
 */

#ifndef MUTATION_H
#define MUTATION_H

/* Include files */
#include <stddef.h>
#include <stdlib.h>
#include "rtwtypes.h"
#include "GACD_types.h"

/* Function Declarations */
extern void Mutation(const emxArray_real_T *W, const emxArray_real_T *degrees,
                     double m, const emxArray_real_T *jin, emxArray_real_T *pop2,
                     emxArray_real_T *pop2_Q, double pop2_size);

#endif

/* End of code generation (Mutation.h) */
